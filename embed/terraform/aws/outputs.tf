output "api_url" {
  description = "E2B_API_URL for the SDK."
  value       = "http://${aws_eip.this.public_ip}:3000"
}

output "sandbox_url" {
  description = "E2B_SANDBOX_URL for the SDK."
  value       = "http://${aws_eip.this.public_ip}:3002"
}

output "dashboard_url" {
  description = "The dashboard, for a browser: sign in with e2b_api_key."
  value       = "http://${aws_eip.this.public_ip}:3001"
}

output "e2b_api_key" {
  description = "E2B_API_KEY for the SDK: the team API key the seed inserted."
  value       = local.team_api_key
  sensitive   = true
}

output "autoscaling_group" {
  description = "Name of the Auto Scaling group of one that holds the instance."
  value       = aws_autoscaling_group.this.name
}

output "session_command" {
  description = "A shell snippet for eval: a Session Manager shell on the group's InService instance, whose id is looked up at run time. Use it as eval \"$(terraform output -raw session_command)\"."
  value       = "aws ssm start-session --region ${local.region} --target $(aws autoscaling describe-auto-scaling-groups --region ${local.region} --auto-scaling-group-names ${aws_autoscaling_group.this.name} --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`] | [0].InstanceId' --output text)"
}
