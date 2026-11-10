provider "azurerm" {
  features {}
  # The module's storage account refuses shared keys, so the provider writes
  # the two blobs with the apply's own identity.
  storage_use_azuread = true
}

module "e2b" {
  source       = "../.."
  client_cidrs = ["203.0.113.0/24"]
}

output "api_url" { value = module.e2b.api_url }
output "sandbox_url" { value = module.e2b.sandbox_url }
output "dashboard_url" { value = module.e2b.dashboard_url }
output "run_command" { value = module.e2b.run_command }
output "e2b_api_key" {
  value     = module.e2b.e2b_api_key
  sensitive = true
}
output "admin_password" {
  value     = module.e2b.admin_password
  sensitive = true
}
