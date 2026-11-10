output "api_url" {
  description = "E2B_API_URL for the SDK."
  value       = "http://${azurerm_public_ip.this.ip_address}:3000"
}

output "sandbox_url" {
  description = "E2B_SANDBOX_URL for the SDK."
  value       = "http://${azurerm_public_ip.this.ip_address}:3002"
}

output "dashboard_url" {
  description = "The dashboard, for a browser: sign in with e2b_api_key."
  value       = "http://${azurerm_public_ip.this.ip_address}:3001"
}

output "e2b_api_key" {
  description = "E2B_API_KEY for the SDK: the team API key the seed inserted."
  value       = local.team_api_key
  sensitive   = true
}

output "scale_set" {
  description = "Name of the scale set of one that holds the instance."
  value       = azurerm_orchestrated_virtual_machine_scale_set.this.name
}

output "run_command" {
  description = "A shell snippet for eval: run a shell script on the instance through the guest agent, whose name is looked up at run time. Use it as eval \"$(terraform output -raw run_command) 'tail -n 50 /var/log/cloud-init-output.log'\"."
  # --subscription on both calls: the active CLI subscription may differ, and a same-named group there would take the script.
  value = "az vm run-command invoke --subscription ${data.azurerm_client_config.current.subscription_id} --resource-group ${azurerm_resource_group.this.name} --name $(az vm list --subscription ${data.azurerm_client_config.current.subscription_id} --resource-group ${azurerm_resource_group.this.name} --query '[0].name' --output tsv) --command-id RunShellScript --scripts"
}

output "admin_password" {
  description = "Password of the e2b user, for the serial console. No port 22 is open."
  value       = random_password.admin.result
  sensitive   = true
}
