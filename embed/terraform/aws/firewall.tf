# The security group denies ingress by default; these rules are the whole
# policy. The stack binds thirteen ports on every interface (api 3000, 5009,
# 5109; dashboard 3001; client-proxy 3002, 3003; dashboard-api 3010;
# orchestrator 5007, 5008 and the sandbox egress proxies 5010, 5016, 5017,
# 5018); 5008 is an unauthenticated control API and the egress proxies and
# dashboard-api expect no external client, so only 3000, 3001 and 3002 are
# opened, and only to the operator's CIDRs. Session Manager reaches the
# instance through its agent's outbound connection, so no port 22 either.
locals {
  client_ports = [3000, 3001, 3002]
}

resource "aws_security_group" "this" {
  name        = var.name
  description = "E2B Embed: the client ports from client_cidrs, all egress"
  vpc_id      = aws_vpc.this.id
  tags        = local.tags
}

# distinct: a CIDR listed twice would otherwise be a duplicate key.
resource "aws_vpc_security_group_ingress_rule" "clients" {
  for_each = {
    for pair in setproduct(local.client_ports, distinct(var.client_cidrs)) :
    "${pair[0]}-${pair[1]}" => { port = pair[0], cidr = pair[1] }
  }

  security_group_id = aws_security_group.this.id
  ip_protocol       = "tcp"
  from_port         = each.value.port
  to_port           = each.value.port
  cidr_ipv4         = each.value.cidr
  tags              = local.tags
}

# Docker Hub, the E2B registries, GitHub and the Ubuntu and Docker apt
# mirrors, the AWS APIs the instance calls itself, and whatever the
# sandboxes reach, which is anywhere.
resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.this.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  tags              = local.tags
}
