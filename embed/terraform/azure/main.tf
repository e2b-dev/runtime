# Everything the module makes lives in its own resource group, so a destroy
# takes the whole install and nothing else in the subscription shares it.
resource "azurerm_resource_group" "this" {
  name     = var.name
  location = var.location
  tags     = var.tags
}

locals {
  team_api_key = var.team_api_key != "" ? var.team_api_key : "e2b_${random_bytes.team_api_key.hex}"
}

# random_bytes, not random_id: its hex attribute is sensitive, so the values
# never appear in `terraform apply` or `terraform show` output. They are in
# the state and in the instance's custom data regardless (README, Secrets).
resource "random_bytes" "admin_token" {
  length = 32
}

resource "random_bytes" "sandbox_access_token_hash_seed" {
  length = 32
}

resource "random_bytes" "team_api_key" {
  length = 16
}

# Azure refuses a Linux instance that has neither a password nor an SSH key.
# No port 22 is opened, so this one is for the serial console only (README,
# Reaching the instance).
resource "random_password" "admin" {
  length           = 32
  min_lower        = 1
  min_upper        = 1
  min_numeric      = 1
  min_special      = 1
  override_special = "-_.~"
}

# The instance's own identity. Everything it may do is the two role
# assignments in iam.tf: read the two shipped files, and move this install's
# public IP onto its own network interface.
resource "azurerm_user_assigned_identity" "this" {
  name                = var.name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = var.tags
}

locals {
  startup_script = templatefile("${path.module}/startup.sh.tftpl", {
    compose_base_url = var.compose_base_url
    blob_host        = azurerm_storage_account.this.primary_blob_host
    container        = azurerm_storage_container.this.name
    # Referencing the blobs orders their upload before any launch, and the
    # hashes make changed files a new scale-set model, as new files make a
    # new launch-template version on AWS. The instance fetches the blobs as
    # they are at boot, so one launched from an older model after the files
    # changed stops at the hash check instead of running files that model
    # never saw.
    compose_blob                   = azurerm_storage_blob.compose_yaml.name
    env_blob                       = azurerm_storage_blob.dot_env.name
    compose_sha256                 = filesha256("${path.module}/../../compose/compose.yaml")
    env_sha256                     = filesha256("${path.module}/../../compose/.env")
    identity_client_id             = azurerm_user_assigned_identity.this.client_id
    public_ip_id                   = azurerm_public_ip.this.id
    resource_group_id              = azurerm_resource_group.this.id
    admin_token                    = random_bytes.admin_token.hex
    sandbox_access_token_hash_seed = random_bytes.sandbox_access_token_hash_seed.hex
    team_api_key                   = local.team_api_key
    hugepages                      = var.hugepages
    dashboard_host                 = azurerm_public_ip.this.ip_address
    # The built-in collector listens on 127.0.0.1:4317, so turning it on
    # points the services there unless an endpoint of the operator's own wins.
    otel_endpoint    = var.otel_collector_grpc_endpoint != "" ? var.otel_collector_grpc_endpoint : (var.otel_collector ? "127.0.0.1:4317" : "")
    compose_profiles = var.otel_collector ? "otel" : ""
  })
}

resource "azurerm_orchestrated_virtual_machine_scale_set" "this" {
  name                = var.name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku_name            = var.instance_size
  instances           = 1
  zones               = var.zone != "" ? [var.zone] : null
  tags                = var.tags

  # Flexible orchestration, which is what this resource is: the instance is
  # an ordinary VM with an interface and disks of its own, which is what lets
  # the first boot attach the public IP to itself. One fault domain is the
  # only count a zonal scale set takes, and the maximum spread for a regional
  # one.
  platform_fault_domain_count = 1
  network_api_version         = "2022-11-01"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.this.id]
  }

  os_profile {
    custom_data = base64encode(local.startup_script)

    # Azure refuses an instance with neither a password nor an SSH key. No
    # port 22 is opened (firewall.tf), so the password reaches the instance
    # through the serial console and nothing else.
    linux_configuration {
      admin_username                  = "e2b"
      admin_password                  = random_password.admin.result
      disable_password_authentication = false
      provision_vm_agent              = true
    }
  }

  source_image_reference {
    publisher = var.image.publisher
    offer     = var.image.offer
    sku       = var.image.sku
    version   = var.image.version
  }

  # Azure encrypts every managed disk at rest with a platform key, so the
  # .env and its three secrets are encrypted without a setting here.
  os_disk {
    caching              = "ReadWrite"
    storage_account_type = var.os_disk_type
    disk_size_gb         = var.os_disk_size_gb
  }

  # No public_ip_address block: the address is created on its own
  # (network.tf) and the instance attaches it, so it survives a replacement.
  # Egress before that attach is the subnet's NAT gateway.
  network_interface {
    name    = var.name
    primary = true

    ip_configuration {
      name      = "internal"
      primary   = true
      subnet_id = azurerm_subnet.this.id
    }
  }

  # The prober on this cloud, in place of the aws module's on-instance
  # watchdog: the extension asks the stack's own /health from inside the
  # instance, so no port is opened for it and no load balancer is involved.
  # Three failed probes 30 seconds apart is unhealthy.
  #
  # Version 1.0 is Binary Health States, where a 200 is healthy and anything
  # else -- another status, a timeout, a refused connection -- is unhealthy.
  # That is the contract /health already meets: it answers 200 with a line of
  # text. Version 2.0 is Rich Health States, which reads the health out of the
  # response BODY and calls a 200 carrying anything else Unknown; repairs
  # treat Unknown as unhealthy, so a healthy node would be replaced every
  # grace period, forever.
  extension {
    name                 = "health"
    publisher            = "Microsoft.ManagedServices"
    type                 = "ApplicationHealthLinux"
    type_handler_version = "1.0"
    settings = jsonencode({
      protocol          = "http"
      port              = 3000
      requestPath       = "/health"
      intervalInSeconds = 30
      numberOfProbes    = 3
    })
  }

  # An instance whose /health stays dead is deleted and replaced, which is
  # delete-before-create, so the address only ever has one holder. The first
  # boot installs Docker and runs the whole first `up` (about four minutes
  # with downloads); the grace period covers it, and starts again for the
  # replacement.
  automatic_instance_repair {
    enabled      = true
    action       = "Replace"
    grace_period = "PT15M"
  }

  # Managed, so `az vm boot-diagnostics get-boot-log` and the serial console
  # both work without a storage account of the operator's own.
  boot_diagnostics {}

  lifecycle {
    # Azure takes at most 64 KB of custom data, base-64 encoded, and the
    # startup script is all of it. tests/terraform.bats keeps the template
    # itself far enough below this that only an extreme variable value can
    # reach it.
    precondition {
      condition     = length(base64encode(local.startup_script)) < 65536
      error_message = "The rendered startup script is over Azure's 64 KB custom-data limit; shorten compose_base_url, team_api_key or otel_collector_grpc_endpoint."
    }
  }

  # The first boot needs egress, which the subnet's NAT gateway is, and the
  # identity's grants, and neither is a reference Terraform can see from
  # here. The security group is in the list so the ports are never open
  # wider than firewall.tf says, not even for the first minute.
  depends_on = [
    azurerm_subnet_nat_gateway_association.this,
    azurerm_subnet_network_security_group_association.this,
    time_sleep.rbac_propagation,
  ]
}
