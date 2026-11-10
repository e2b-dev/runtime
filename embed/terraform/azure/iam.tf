data "azurerm_client_config" "current" {}

# Anyone with a shell on the instance, and any process on the host, holds the
# identity, so these two grants are the blast radius of a compromised install:
# read the two shipped blobs, and move this install's public IP onto its own
# network interface.
resource "azurerm_role_assignment" "blob_reader" {
  scope                = azurerm_storage_container.this.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.this.principal_id
  principal_type       = "ServicePrincipal"
}

# The built-in role that can attach a public IP is Network Contributor, which
# also carries every other write in Microsoft.Network. These five actions are
# what the first boot's GET and PUT of its own interface need and nothing
# else: a network interface is written whole, so attaching the address also
# re-states the subnet the interface is already in, which Azure authorizes
# with a join.
resource "azurerm_role_definition" "public_ip_attach" {
  name        = "${var.name}-public-ip-attach"
  scope       = azurerm_resource_group.this.id
  description = "Attach this install's public IP to its own instance's network interface."

  permissions {
    actions = [
      "Microsoft.Network/networkInterfaces/read",
      "Microsoft.Network/networkInterfaces/write",
      "Microsoft.Network/publicIPAddresses/read",
      "Microsoft.Network/publicIPAddresses/join/action",
      "Microsoft.Network/virtualNetworks/subnets/join/action",
    ]
    not_actions = []
  }

  assignable_scopes = [azurerm_resource_group.this.id]
}

# The resource group, not the address: the interface the instance writes does
# not exist until the scale set launches it, so there is no narrower scope to
# name it at. The group holds this install and nothing else.
resource "azurerm_role_assignment" "public_ip_attach" {
  scope              = azurerm_resource_group.this.id
  role_definition_id = azurerm_role_definition.public_ip_attach.role_definition_resource_id
  principal_id       = azurerm_user_assigned_identity.this.principal_id
  principal_type     = "ServicePrincipal"
}

# Writing the two blobs is a data-plane call, and the account takes no keys,
# so whoever runs `terraform apply` needs the data-plane role too. Reading
# them is the identity's grant above; this one is the apply's.
resource "azurerm_role_assignment" "blob_writer" {
  scope                = azurerm_storage_container.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Role assignments are eventually consistent: a data-plane call or a first
# boot inside the window after one is written is refused as if it were never
# made. The blobs and the scale set wait on this sleep, which starts once all
# three assignments exist and starts again if one of them is replaced.
resource "time_sleep" "rbac_propagation" {
  create_duration = "60s"
  triggers = {
    blob_reader      = azurerm_role_assignment.blob_reader.id
    blob_writer      = azurerm_role_assignment.blob_writer.id
    public_ip_attach = azurerm_role_assignment.public_ip_attach.id
  }
}
