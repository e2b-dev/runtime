variable "name" {
  description = "Name prefix for every resource."
  type        = string
  default     = "e2b-embed"
  validation {
    condition     = can(regex("^[a-z][-a-z0-9]{4,28}[a-z0-9]$", var.name))
    error_message = "name must be 6-30 characters: lowercase letters, digits and hyphens, starting with a letter and not ending in a hyphen (it prefixes the bucket name, which S3 wants lowercase)."
  }
  # The bucket name is name and a hyphen, then a suffix Terraform picks, so
  # the check reads that prefix: a name of sthree makes a bucket sthree-...
  validation {
    condition     = !can(regex("^(xn--|sthree-|amzn-s3-demo-)", "${var.name}-"))
    error_message = "name must not be sthree or amzn-s3-demo, or start with xn--, sthree- or amzn-s3-demo-: the bucket name starts with name and a hyphen, and S3 reserves those prefixes."
  }
}

variable "instance_type" {
  description = "Instance type. It must expose /dev/kvm to the instance: a nested-virtualization type or an x86-64 .metal type. AWS supports nested virtualization on the 7th- and 8th-generation Intel families (m7i, c7i and r7i, m8i, c8i and r8i, and the -flex variants AWS offers of them); this module was validated on m8i.xlarge. 12 GiB RAM recommended; the default has 16."
  type        = string
  default     = "m8i.xlarge"
  # Sandboxes are Firecracker microVMs. On a virtual instance only these
  # families offer nested virtualization (AWS documents it for the 7th and
  # the 8th generation alike; this module was validated on m8i.xlarge); a
  # .metal type has /dev/kvm directly. Graviton and the other arm64 types
  # have no nested virtualization, Mac types run only on dedicated hosts,
  # and the default AMI is x86-64 Ubuntu in any case.
  validation {
    condition     = can(regex("^([cmr][78]i(-flex)?\\.([0-9]*x)?large|[a-z0-9-]+\\.metal(-[0-9]+xl)?)$", var.instance_type))
    error_message = "instance_type must be a nested-virtualization type such as m8i.xlarge or an x86-64 .metal type: sandboxes are Firecracker microVMs and need /dev/kvm on the instance. AWS supports nested virtualization on the 7th- and 8th-generation Intel families (m7i, c7i and r7i, m8i, c8i and r8i, and the -flex variants AWS offers of them); this module was validated on m8i.xlarge."
  }
  validation {
    condition     = !can(regex("^([a-z]+[0-9]+g|a1\\.|mac)", var.instance_type))
    error_message = "instance_type must not be an arm64 type (Graviton: a1, or a family with a g after its generation, such as m7g) or a Mac type: arm64 instances offer no nested virtualization, Mac instances run only on dedicated hosts, and the module's default AMI is x86-64 Ubuntu."
  }
}

variable "availability_zone" {
  description = "Availability zone for the subnet and the instance: one of the region's own, not a Local or Wavelength Zone, and it must offer instance_type. Empty picks the first zone of the provider's region that offers instance_type. Read when the subnet is created, not after (network.tf says why)."
  type        = string
  default     = ""
}

variable "vpc_cidr" {
  description = "IPv4 CIDR of the dedicated VPC, /16 to /20; the subnet is its first block 8 bits longer (a /24 of the default). It must not overlap 10.11.0.0/16 or 10.12.0.0/16, the orchestrator's sandbox networks on the instance. Read when the VPC is created: changing it on an existing install fails at apply, so destroy the install first."
  type        = string
  default     = "10.10.0.0/16"
  # A network address, as client_cidrs takes below: 10.10.5.0/16 is refused
  # here, with the reason, rather than by the provider.
  validation {
    condition     = can(cidrnetmask(var.vpc_cidr)) && try(cidrsubnet(var.vpc_cidr, 0, 0) == var.vpc_cidr && tonumber(split("/", var.vpc_cidr)[1]) >= 16 && tonumber(split("/", var.vpc_cidr)[1]) <= 20, false)
    error_message = "vpc_cidr must be an IPv4 network address from /16 to /20, such as 10.10.0.0/16."
  }
  # At /16 or longer a CIDR lies inside one /16, so its first two octets say
  # whether it overlaps either of the orchestrator's.
  validation {
    condition     = !contains(["10.11", "10.12"], try(join(".", slice(split(".", cidrhost(var.vpc_cidr, 0)), 0, 2)), ""))
    error_message = "vpc_cidr must not overlap 10.11.0.0/16 or 10.12.0.0/16, the orchestrator's sandbox networks on the instance."
  }
}

variable "client_cidrs" {
  description = "IPv4 CIDRs allowed to reach the API (3000), the dashboard (3001) and sandbox proxy (3002). Nothing else is reachable from outside. Each CIDR takes three of the security group's inbound rules, 60 by default, so up to 20 CIDRs unless the account's quota is raised."
  type        = list(string)
  validation {
    condition     = length(var.client_cidrs) > 0
    error_message = "client_cidrs must list at least one CIDR; the stack is unusable without a client."
  }
  # cidrnetmask accepts IPv4 prefixes only, and the VPC has no IPv6. The
  # rules take a network address only: cidrsubnet(c, 0, 0) is c with its
  # host bits cleared, so 203.0.113.5/24 is refused here, with the reason,
  # rather than by the provider.
  validation {
    condition     = alltrue([for c in var.client_cidrs : can(cidrnetmask(c)) && try(cidrsubnet(c, 0, 0) == c, false)])
    error_message = "Every client_cidrs entry must be an IPv4 network address such as 203.0.113.0/24 or 198.51.100.7/32; the VPC has no IPv6."
  }
}

variable "tags" {
  description = "Tags applied to every resource that takes them except the two bucket objects (S3 allows an object only 10), and to the instance, its volume and its network interface through the launch template."
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

variable "ami" {
  description = "AMI id. Empty uses Canonical's current Ubuntu 24.04 LTS amd64 image, read from its public SSM parameter at each plan; a set id skips that read. It must be known at plan time (a literal, a variable or a data source read at plan), because it decides whether the parameter is read; an id from a resource created in the same apply fails the plan with Invalid count argument. The stack requires Ubuntu 24.04 (apt, writable /etc, glibc >= 2.34) on x86-64, and the root volume settings apply to /dev/sda1, the root device of Canonical's images."
  type        = string
  default     = ""
}

variable "root_volume_size_gb" {
  description = "Root volume size. The stack needs 20 GiB free after the OS and Docker."
  type        = number
  default     = 50
}

variable "root_volume_type" {
  description = "Root volume type."
  type        = string
  default     = "gp3"
}
