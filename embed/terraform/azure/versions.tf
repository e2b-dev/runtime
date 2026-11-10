terraform {
  required_version = ">= 1.7.5"
  required_providers {
    # 4.0 is the first release where azurerm_storage_container reaches the
    # container through Resource Manager, which is what lets the storage
    # account refuse shared keys (storage.tf).
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.0, < 5.0"
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
