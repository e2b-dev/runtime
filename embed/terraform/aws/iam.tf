data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  arn_prefix = "arn:${data.aws_partition.current.partition}"
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region
}

# The instance's own identity. Everything it may do is the inline policy
# below: read the two shipped files, claim its Elastic IP, report itself
# unhealthy to its group, and run the SSM agent for Session Manager and Run
# Command.
resource "aws_iam_role" "this" {
  name = var.name
  tags = local.tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_instance_profile" "this" {
  name = var.name
  role = aws_iam_role.this.name
  tags = local.tags
}

# IAM is eventually consistent: EC2 can refuse a new instance profile for
# some seconds after IAM returns it, and a launch that names it in that
# window fails with "Authentication Failure". The launch template and the
# group wait on this sleep, which starts once the profile and the role's
# policy both exist. A profile or policy replaced under a new name has a new
# ARN or id, which replaces the sleep, so it waits again.
resource "time_sleep" "iam_propagation" {
  create_duration = "30s"
  triggers = {
    profile = aws_iam_instance_profile.this.arn
    policy  = aws_iam_role_policy.this.id
  }
}

resource "aws_iam_role_policy" "this" {
  name = var.name
  role = aws_iam_role.this.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadTheShippedFiles"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.this.arn}/${aws_s3_object.compose_yaml.key}",
          "${aws_s3_bucket.this.arn}/${aws_s3_object.dot_env.key}",
        ]
      },
      {
        # AssociateAddress is authorized against the address and against
        # the instance and its interface. The address is this install's
        # own; the instance is not known until the group launches it, so
        # the next two statements match the instance by this install's
        # profile and its interface by this install's VPC.
        Sid      = "ClaimTheElasticIp"
        Effect   = "Allow"
        Action   = ["ec2:AssociateAddress"]
        Resource = ["${local.arn_prefix}:ec2:${local.region}:${local.account_id}:elastic-ip/${aws_eip.this.allocation_id}"]
      },
      {
        Sid      = "OntoThisInstallsInstance"
        Effect   = "Allow"
        Action   = ["ec2:AssociateAddress"]
        Resource = ["${local.arn_prefix}:ec2:${local.region}:${local.account_id}:instance/*"]
        Condition = {
          ArnEquals = { "ec2:InstanceProfile" = aws_iam_instance_profile.this.arn }
        }
      },
      {
        Sid      = "OntoThisInstallsInterface"
        Effect   = "Allow"
        Action   = ["ec2:AssociateAddress"]
        Resource = ["${local.arn_prefix}:ec2:${local.region}:${local.account_id}:network-interface/*"]
        Condition = {
          ArnEquals = { "ec2:Vpc" = aws_vpc.this.arn }
        }
      },
      {
        # By name, not by reference: the group depends on this policy, so
        # the permission exists before the group launches an instance.
        Sid      = "ReportHealthToTheGroup"
        Effect   = "Allow"
        Action   = ["autoscaling:SetInstanceHealth"]
        Resource = ["${local.arn_prefix}:autoscaling:${local.region}:${local.account_id}:autoScalingGroup:*:autoScalingGroupName/${var.name}"]
      },
      {
        # What the SSM agent calls for Session Manager and Run Command: its
        # heartbeat, the ssmmessages channels that carry sessions and, on
        # agents from 3.3.40 on, commands too, and the ec2messages calls older
        # agents and regions use for commands.
        # Written out rather than AWS's AmazonSSMManagedInstanceCore policy,
        # which also reads any Parameter Store parameter in the account and
        # region by name, for any process on the host. ssmmessages and
        # ec2messages have no resource to name, and the instance
        # UpdateInstanceInformation names does not exist when the policy is
        # written; so all are on every resource, as in AWS's policy.
        Sid    = "RunTheSsmAgent"
        Effect = "Allow"
        Action = [
          "ssm:UpdateInstanceInformation",
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel",
          "ec2messages:AcknowledgeMessage",
          "ec2messages:DeleteMessage",
          "ec2messages:FailMessage",
          "ec2messages:GetEndpoint",
          "ec2messages:GetMessages",
          "ec2messages:SendReply",
        ]
        Resource = ["*"]
      },
    ]
  })
}
