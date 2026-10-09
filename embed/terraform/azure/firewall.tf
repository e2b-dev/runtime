# The stack binds thirteen ports on every interface (api 3000, 5009, 5109;
# dashboard 3001; client-proxy 3002, 3003; dashboard-api 3010; orchestrator
# 5007, 5008 and the sandbox egress proxies 5010, 5016, 5017, 5018); 5008 is
# an unauthenticated control API and the egress proxies and dashboard-api
# expect no external client, so only 3000, 3001 and 3002 are opened, and only
# to the operator's CIDRs. There is no port 22: the serial console and Run
# Command reach the instance through the platform, not through the network.
# Egress stays as Azure leaves it, open. The deny rule exists because the
# group's own defaults are not deny-all: AllowVNetInBound admits every port
# from the whole VirtualNetwork tag — the VNet plus anything peered to it or
# connected on-premises — ahead of the final deny, so the moment this network
# gains private connectivity the surface would silently widen past the three
# ports. The deny stands between the clients rule and the defaults.
locals {
  client_ports = [3000, 3001, 3002]
}

resource "azurerm_network_security_group" "this" {
  name                = var.name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = var.tags
}

# distinct: a CIDR listed twice would otherwise be refused as a duplicate.
resource "azurerm_network_security_rule" "clients" {
  name                        = "clients"
  resource_group_name         = azurerm_resource_group.this.name
  network_security_group_name = azurerm_network_security_group.this.name
  priority                    = 100
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_ranges     = [for port in local.client_ports : tostring(port)]
  source_address_prefixes     = distinct(var.client_cidrs)
  destination_address_prefix  = "*"
}

resource "azurerm_network_security_rule" "deny_inbound" {
  name                        = "deny-inbound"
  resource_group_name         = azurerm_resource_group.this.name
  network_security_group_name = azurerm_network_security_group.this.name
  priority                    = 4096
  direction                   = "Inbound"
  access                      = "Deny"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "*"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
}

resource "azurerm_subnet_network_security_group_association" "this" {
  subnet_id                 = azurerm_subnet.this.id
  network_security_group_id = azurerm_network_security_group.this.id
}
