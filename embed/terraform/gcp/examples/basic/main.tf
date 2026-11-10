module "e2b" {
  source       = "../.."
  project_id   = "my-project"
  client_cidrs = ["203.0.113.0/24"]
}

output "api_url" { value = module.e2b.api_url }
output "sandbox_url" { value = module.e2b.sandbox_url }
output "dashboard_url" { value = module.e2b.dashboard_url }
output "ssh_command" { value = module.e2b.ssh_command }
output "e2b_api_key" {
  value     = module.e2b.e2b_api_key
  sensitive = true
}
