variable "name" {
  description = "Name prefix for every resource."
  type        = string
  default     = "e2b-embed"
  validation {
    condition     = can(regex("^[a-z][-a-z0-9]{4,28}[a-z0-9]$", var.name))
    error_message = "name must be 6-30 characters: lowercase letters, digits and hyphens, starting with a letter and not ending in a hyphen (it prefixes the storage account name, which Azure wants lowercase)."
  }
}

variable "location" {
  description = "Azure region for the resource group and everything in it. It must offer instance_size, and zone unless that is empty."
  type        = string
  default     = "eastus"
}

variable "zone" {
  description = "Availability zone for the instance, as a bare number. Empty places it regionally, for a region that has no zones; the public IP is zone-redundant either way."
  type        = string
  default     = "1"
  validation {
    condition     = contains(["", "1", "2", "3"], var.zone)
    error_message = "zone must be empty, 1, 2 or 3."
  }
}

variable "instance_size" {
  description = "VM size. It must expose /dev/kvm to the instance, which on Azure is decided by the size alone: there is no per-instance nested-virtualization flag and no plan-time API to ask. Take a general-purpose D size with premium storage -- generation 3 or newer for the Intel sizes (Dsv3 and up, with or without the d letter), generation 5 or newer for the AMD sizes (an a in the name: Dasv5 and up, where nested virtualization first appears) -- or an Lsv3 and up; those are the x86-64 families Azure offers nested virtualization on. Which generations a subscription may use varies by region and by the subscription's own offer -- `az vm list-skus -l <region> --resource-type virtualMachines` is what answers it. 12 GiB RAM recommended; the default has 16."
  type        = string
  default     = "Standard_D4ds_v7"
  # Sandboxes are Firecracker microVMs, so the host needs /dev/kvm. The
  # burstable sizes do not offer nested virtualization and the Cobalt sizes
  # (a p in the size name) are arm64, which the default image is not.
  validation {
    condition     = can(regex("^Standard_(D[0-9]+d?s_v[3-9]|D[0-9]+ad?s_v[5-9]|L[0-9]+s_v[3-9])$", var.instance_size))
    error_message = "instance_size must be a general-purpose D size with premium storage -- Intel generation 3 or newer (such as Standard_D4ds_v5), AMD generation 5 or newer (an a in the name; the AMD v4 sizes have no nested virtualization) -- or an Lsv3 and up: sandboxes are Firecracker microVMs and need /dev/kvm on the instance, which the size alone decides."
  }
  validation {
    condition     = !can(regex("^Standard_(B|[A-Z]+[0-9]+[a-z]*p)", var.instance_size))
    error_message = "instance_size must not be a burstable size (Standard_B...) or an arm64 Cobalt size (a p in the size name, such as Standard_D4ps_v5): burstable sizes offer no nested virtualization, and the module's default image is x86-64 Ubuntu."
  }
}

variable "image" {
  description = "Marketplace image for the instance. The stack requires Ubuntu 24.04 (apt, writable /etc, glibc >= 2.34) on x86-64, and a generation 2 image, which is what the v5 and newer sizes take. A version of latest records a new scale-set model when Canonical publishes an image; the running instance is replaced only as Upgrading describes."
  type = object({
    publisher = string
    offer     = string
    sku       = string
    version   = string
  })
  default = {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }
}

variable "os_disk_size_gb" {
  description = "OS disk size. The stack needs 20 GiB free after the OS and Docker."
  type        = number
  default     = 50
}

variable "os_disk_type" {
  description = "OS disk type. Azure encrypts every managed disk at rest with a platform key; the README's Secrets section says what that covers."
  type        = string
  default     = "Premium_LRS"
}

variable "client_cidrs" {
  description = "IPv4 CIDRs allowed to reach the API (3000), the dashboard (3001) and sandbox proxy (3002). Nothing else is reachable from outside. One network security group rule carries all of them, so the count is not limited the way a per-rule cloud limits it."
  type        = list(string)
  validation {
    condition     = length(var.client_cidrs) > 0
    error_message = "client_cidrs must list at least one CIDR; the stack is unusable without a client."
  }
  # cidrnetmask accepts IPv4 prefixes only, and the network has no IPv6. The
  # rule takes a network address only: cidrsubnet(c, 0, 0) is c with its host
  # bits cleared, so 203.0.113.5/24 is refused here, with the reason, rather
  # than by the provider.
  validation {
    condition     = alltrue([for c in var.client_cidrs : can(cidrnetmask(c)) && try(cidrsubnet(c, 0, 0) == c, false)])
    error_message = "Every client_cidrs entry must be an IPv4 network address such as 203.0.113.0/24 or 198.51.100.7/32; the network has no IPv6."
  }
}

variable "tags" {
  description = "Tags applied to every resource in the module's resource group."
  type        = map(string)
  default     = {}
}

variable "compose_base_url" {
  description = "Optional directory URL to fetch compose.yaml and .env from at first boot instead of the copies embedded from the package's compose directory."
  type        = string
  default     = ""
  validation {
    condition     = var.compose_base_url == "" || can(regex("^https?://", var.compose_base_url))
    error_message = "compose_base_url must be empty or start with http:// or https://."
  }
}

variable "team_api_key" {
  description = "Optional team API key: e2b_ followed by an even number of lowercase hex characters, at least 32 (the seed hex-decodes it and wants 16 bytes or more). Empty generates one; read it with `terraform output -raw e2b_api_key`."
  type        = string
  default     = ""
  sensitive   = true
  validation {
    condition     = var.team_api_key == "" || can(regex("^e2b_([0-9a-f]{2}){16,}$", var.team_api_key))
    error_message = "team_api_key must be empty or e2b_ followed by an even number of lowercase hex characters, at least 32."
  }
}

variable "hugepages" {
  description = "2 MiB hugepages reserved for sandboxes (HUGEPAGES in .env). 2048 is 4 GiB."
  type        = number
  default     = 2048
}

variable "otel_collector_grpc_endpoint" {
  description = "Optional host:port of an OTLP/gRPC collector the services export metrics, traces and logs to (E2B_OTEL_COLLECTOR_GRPC_ENDPOINT in .env). Empty disables export unless otel_collector is true, which implies 127.0.0.1:4317."
  type        = string
  default     = ""
  # The services take host:port and nothing else, and the value lands in the
  # instance's .env at first boot, where a typo would cost a replace to fix.
  validation {
    condition     = var.otel_collector_grpc_endpoint == "" || can(regex("^(\\[[0-9A-Fa-f:]+\\]|[A-Za-z0-9.-]+):[0-9]{1,5}$", var.otel_collector_grpc_endpoint))
    error_message = "otel_collector_grpc_endpoint must be empty or host:port, with no scheme."
  }
}

variable "otel_collector" {
  description = "Run the built-in OpenTelemetry collector on the instance, writing sandbox and team metrics into the stack's own ClickHouse. Implies the endpoint 127.0.0.1:4317 when otel_collector_grpc_endpoint is empty."
  type        = bool
  default     = false
}
