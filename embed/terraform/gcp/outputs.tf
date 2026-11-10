output "api_url" {
  description = "E2B_API_URL for the SDK."
  value       = "http://${google_compute_address.this.address}:3000"
}

output "sandbox_url" {
  description = "E2B_SANDBOX_URL for the SDK."
  value       = "http://${google_compute_address.this.address}:3002"
}

output "dashboard_url" {
  description = "The dashboard, for a browser: sign in with e2b_api_key."
  value       = "http://${google_compute_address.this.address}:3001"
}

output "e2b_api_key" {
  description = "E2B_API_KEY for the SDK: the team API key the seed inserted."
  value       = local.team_api_key
  sensitive   = true
}

output "instance_group" {
  description = "Self link of the managed instance group."
  value       = google_compute_instance_group_manager.this.self_link
}

output "ssh_command" {
  description = "A shell snippet for eval: SSH through IAP to the current instance, whose name is looked up at run time. Use it as eval \"$(terraform output -raw ssh_command)\"."
  value       = "gcloud compute ssh --project ${var.project_id} --zone ${var.zone} --tunnel-through-iap $(gcloud compute instance-groups managed list-instances ${var.name} --project ${var.project_id} --zone ${var.zone} --format='value(instance.basename())')"
}
