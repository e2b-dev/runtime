variable "project_id" {
  description = "GCP project that hosts the stack. Compute Engine API must be enabled."
  type        = string
}

variable "zone" {
  description = "Zone for the instance group. The region for the subnet and the reserved address is derived from it. Must support the machine type and nested virtualization."
  type        = string
  default     = "us-west1-b"
  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]+-[a-z]$", var.zone))
    error_message = "zone must look like us-west1-b."
  }
}

variable "name" {
  description = "Name prefix for every resource."
  type        = string
  default     = "e2b-embed"
  validation {
    condition     = can(regex("^[a-z][-a-z0-9]{4,28}[a-z0-9]$", var.name))
    error_message = "name must be 6-30 characters: lowercase letters, digits and hyphens, starting with a letter and not ending in a hyphen (it becomes the service account id)."
  }
}

variable "machine_type" {
  description = "Instance machine type. 12 GiB RAM recommended; the default has 16."
  type        = string
  default     = "n4-standard-4"
}

variable "boot_disk_size_gb" {
  description = "Boot disk size. The stack needs 20 GiB free after the OS and Docker."
  type        = number
  default     = 50
}

variable "boot_disk_type" {
  description = "Boot disk type. n4 machine types support only Hyperdisk."
  type        = string
  default     = "hyperdisk-balanced"
}

variable "image" {
  description = "Boot image. The stack requires Ubuntu 24.04 (apt, writable /etc, glibc >= 2.34). x86-64 only: the startup script installs Docker's amd64 apt repository, and GCE's arm64 machine types have no nested virtualization, so no /dev/kvm."
  type        = string
  default     = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64"
}

variable "client_cidrs" {
  description = "CIDRs allowed to reach the API (3000) and sandbox proxy (3002). Nothing else is reachable from outside."
  type        = list(string)
  validation {
    condition     = length(var.client_cidrs) > 0
    error_message = "client_cidrs must list at least one CIDR; the stack is unusable without a client."
  }
  validation {
    condition     = alltrue([for c in var.client_cidrs : can(cidrhost(c, 0))])
    error_message = "Every client_cidrs entry must be a CIDR such as 203.0.113.0/24."
  }
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

variable "labels" {
  description = "Labels applied to the instance template and address."
  type        = map(string)
  default     = {}
}
