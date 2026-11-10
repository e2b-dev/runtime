# A dedicated VPC with one public subnet, so nothing else in the account
# shares the instance's security group or route table.
resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = local.tags
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = local.tags
}

resource "aws_subnet" "this" {
  vpc_id            = aws_vpc.this.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 0)
  availability_zone = local.availability_zone
  tags              = local.tags

  # The zone is chosen once, at create time. When availability_zone is empty
  # it is the first zone that offers the instance type, and a zone AWS adds
  # to that list later must not move the subnet, which would replace the
  # instance with it. To move it on purpose, -replace the subnet: the Auto
  # Scaling group is replaced with it (main.tf), so the instance boots again
  # from a fresh volume and rebuilds base. A type changed later is checked
  # against the zone the subnet is in.
  lifecycle {
    ignore_changes = [availability_zone]
    postcondition {
      condition     = contains(data.aws_ec2_instance_type_offerings.this.locations, self.availability_zone)
      error_message = "The subnet's zone, ${self.availability_zone}, does not offer instance_type ${var.instance_type}; choose a type it offers, or -replace the module's aws_subnet.this to move the subnet to ${var.availability_zone != "" ? var.availability_zone : "the first zone that offers the type"}. Moving the subnet replaces the instance, which boots from a fresh volume and rebuilds base."
    }
  }
}

resource "aws_route_table" "this" {
  vpc_id = aws_vpc.this.id
  tags   = local.tags
}

resource "aws_route" "internet" {
  route_table_id         = aws_route_table.this.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "this" {
  subnet_id      = aws_subnet.this.id
  route_table_id = aws_route_table.this.id
}

# The stable address: E2B_DASHBOARD_HOST and the three URL outputs name it
# before any instance exists, and each instance associates it to itself on
# first boot (an Auto Scaling group cannot attach one).
resource "aws_eip" "this" {
  domain = "vpc"
  tags   = local.tags

  depends_on = [aws_internet_gateway.this]
}
