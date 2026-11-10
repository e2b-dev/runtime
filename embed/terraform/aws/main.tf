# Local and Wavelength Zones are listed as zones too once an account opts in,
# and sort before the region's own; the Elastic IP comes from the region's
# address pool, which an instance in one of them cannot use.
data "aws_availability_zones" "regional" {
  filter {
    name   = "zone-type"
    values = ["availability-zone"]
  }
}

# The region's own zones that offer the instance type: not all of them do
# (m8i.xlarge is absent from one of us-east-1's). An explicit
# availability_zone is checked against them too, so a zone without the type
# fails the plan rather than every launch the group makes after it.
data "aws_ec2_instance_type_offerings" "this" {
  location_type = "availability-zone"
  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
  filter {
    name   = "location"
    values = data.aws_availability_zones.regional.names
  }
  lifecycle {
    postcondition {
      condition     = var.availability_zone == "" ? length(self.locations) > 0 : contains(self.locations, var.availability_zone)
      error_message = "instance_type is not offered in ${var.availability_zone != "" ? var.availability_zone : "any zone of this region"}; the zones that offer it: ${length(self.locations) > 0 ? join(", ", sort(self.locations)) : "none, so choose another instance_type or region"}."
    }
  }
}

locals {
  tags              = merge(var.tags, { Name = var.name })
  availability_zone = var.availability_zone != "" ? var.availability_zone : sort(data.aws_ec2_instance_type_offerings.this.locations)[0]
}

locals {
  team_api_key = var.team_api_key != "" ? var.team_api_key : "e2b_${random_bytes.team_api_key.hex}"
  startup_script = templatefile("${path.module}/startup.sh.tftpl", {
    compose_base_url = var.compose_base_url
    s3_host          = aws_s3_bucket.this.bucket_regional_domain_name
    # Referencing the objects orders their upload before any launch, and
    # the hashes make new files a new launch-template version, as new files
    # make a new instance template on GCP. The instance fetches the objects
    # as they are at boot, so one launched from an older version after the
    # files changed stops at the hash check instead of running files that
    # version never saw.
    compose_key                    = aws_s3_object.compose_yaml.key
    env_key                        = aws_s3_object.dot_env.key
    compose_sha256                 = filesha256("${path.module}/../../compose/compose.yaml")
    env_sha256                     = filesha256("${path.module}/../../compose/.env")
    eip_allocation_id              = aws_eip.this.allocation_id
    admin_token                    = random_bytes.admin_token.hex
    sandbox_access_token_hash_seed = random_bytes.sandbox_access_token_hash_seed.hex
    team_api_key                   = local.team_api_key
    hugepages                      = var.hugepages
    dashboard_host                 = aws_eip.this.public_ip
    # The built-in collector listens on 127.0.0.1:4317, so turning it on
    # points the services there unless an endpoint of the operator's own wins.
    otel_endpoint    = var.otel_collector_grpc_endpoint != "" ? var.otel_collector_grpc_endpoint : (var.otel_collector ? "127.0.0.1:4317" : "")
    compose_profiles = var.otel_collector ? "otel" : ""
  })
}

# random_bytes, not random_id: its hex attribute is sensitive, so the values
# never appear in `terraform apply` or `terraform show` output. They are in
# the state and in the instance's user data regardless (README, Secrets).
resource "random_bytes" "admin_token" {
  length = 32
}

resource "random_bytes" "sandbox_access_token_hash_seed" {
  length = 32
}

resource "random_bytes" "team_api_key" {
  length = 16
}

# Canonical publishes the current Ubuntu 24.04 AMI id of each region under
# this public parameter. It is read only when ami is empty, so an explicit ami
# plans in an account that may not read it; the count is also why ami has to
# be known at plan time. insecure_value: the id is not a secret, and value
# would mark the launch template's image sensitive in every plan.
data "aws_ssm_parameter" "ubuntu" {
  count = var.ami == "" ? 1 : 0
  name  = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

locals {
  ami = var.ami != "" ? var.ami : one(data.aws_ssm_parameter.ubuntu[*].insecure_value)
  # A .metal type has /dev/kvm directly and takes no nested-virtualization
  # flag.
  metal = can(regex("\\.metal", var.instance_type))
}

resource "aws_launch_template" "this" {
  name_prefix            = "${var.name}-"
  image_id               = local.ami
  instance_type          = var.instance_type
  update_default_version = true
  user_data              = base64encode(local.startup_script)
  tags                   = local.tags

  iam_instance_profile {
    arn = aws_iam_instance_profile.this.arn
  }

  # The launch-time public address carries the first downloads and the
  # AssociateAddress call; the Elastic IP then replaces it.
  network_interfaces {
    device_index                = 0
    associate_public_ip_address = true
    security_groups             = [aws_security_group.this.id]
    delete_on_termination       = true
  }

  # Ubuntu's AMIs name their root device /dev/sda1.
  block_device_mappings {
    device_name = "/dev/sda1"
    ebs {
      volume_size           = var.root_volume_size_gb
      volume_type           = var.root_volume_type
      encrypted             = true
      delete_on_termination = true
    }
  }

  # Sandboxes are Firecracker microVMs; the instance itself must expose KVM.
  dynamic "cpu_options" {
    for_each = local.metal ? [] : [1]
    content {
      nested_virtualization = "enabled"
    }
  }

  # IMDSv2 only, one hop: the host reaches it, and so does everything on the
  # host network: every compose service with network_mode: host (api,
  # orchestrator, client-proxy, dashboard and the rest), the Session Manager
  # agent, the startup script and the watchdog. Containers on Docker's bridge
  # network do not, and sandboxes never do (Firecracker answers that address
  # inside the guest).
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  tag_specifications {
    resource_type = "instance"
    tags          = local.tags
  }

  tag_specifications {
    resource_type = "volume"
    tags          = local.tags
  }

  tag_specifications {
    resource_type = "network-interface"
    tags          = local.tags
  }

  lifecycle {
    # EC2 takes at most 16 KB of user data; the script is ASCII, so its
    # length is its size. tests/terraform.bats keeps the template itself far
    # enough below this that only an extreme variable value can reach it.
    precondition {
      condition     = length(local.startup_script) < 16384
      error_message = "The rendered startup script is over EC2's 16 KB user-data limit; shorten compose_base_url, team_api_key or otel_collector_grpc_endpoint."
    }
  }

  # The profile's ARN is known as soon as IAM creates it, before EC2 accepts
  # it (iam.tf).
  depends_on = [time_sleep.iam_propagation]
}

resource "aws_autoscaling_group" "this" {
  name                = var.name
  min_size            = 1
  max_size            = 1
  desired_capacity    = 1
  vpc_zone_identifier = [aws_subnet.this.id]

  # A new launch-template version (new files, new secrets, a new AMI) is
  # recorded here but never replaces the running instance by itself; the
  # operator starts an instance refresh (README, Upgrading). A replacement
  # the group makes on its own uses this version too.
  launch_template {
    id      = aws_launch_template.this.id
    version = aws_launch_template.this.latest_version
  }

  # EC2 status checks only: there is no load balancer. The instance's own
  # watchdog (startup.sh.tftpl) reports it Unhealthy when /health has failed
  # for ten minutes. The first boot installs Docker and runs the whole first
  # `up` (about four minutes with downloads); the grace period covers it.
  health_check_type         = "EC2"
  health_check_grace_period = 900

  # One instance holding one Elastic IP: a replacement, for health or for a
  # refresh, terminates the old instance before it launches the new one.
  instance_maintenance_policy {
    min_healthy_percentage = 0
    max_healthy_percentage = 100
  }

  # The instance, its volume and its network interface take their tags from
  # the launch template.
  dynamic "tag" {
    for_each = local.tags
    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = false
    }
  }

  # The instance's network interface sits in the subnet and the security
  # group, so neither can be deleted while the instance runs. Replacing
  # either replaces this Auto Scaling group first, which terminates the
  # instance before the old subnet or security group is deleted; the next
  # instance boots from a fresh volume and rebuilds base. A tag change keeps
  # both ids and replaces nothing.
  lifecycle {
    replace_triggered_by = [aws_subnet.this.id, aws_security_group.this.id]
  }

  # The first boot needs the role's permissions, a route to the internet and
  # the egress rule (the provider removes a new security group's default
  # one), and none is a reference Terraform can see from here. The launch
  # itself needs EC2 to accept the instance profile, which the sleep in
  # iam.tf waits for.
  depends_on = [
    aws_iam_role_policy.this,
    aws_route.internet,
    aws_route_table_association.this,
    aws_vpc_security_group_egress_rule.all,
    time_sleep.iam_propagation,
  ]
}
