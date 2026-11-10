terraform {
  required_version = ">= 1.7.5"
  required_providers {
    # 6.33 is the first release with cpu_options.nested_virtualization on
    # aws_launch_template.
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.33, < 7.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.6"
    }
    time = {
      source  = "hashicorp/time"
      version = ">= 0.12"
    }
  }
}
