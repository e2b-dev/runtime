# A dedicated virtual network with one subnet, so nothing else in the
# subscription shares the instance's security group or its route table.
resource "azurerm_virtual_network" "this" {
  name                = var.name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  address_space       = ["10.10.0.0/24"]
  tags                = var.tags
}

resource "azurerm_subnet" "this" {
  name                 = var.name
  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = ["10.10.0.0/24"]
}

# The stable address: E2B_DASHBOARD_HOST and the three URL outputs name it
# before any instance exists, and each instance attaches it to its own
# network interface on first boot (a scale set cannot attach one). Standing
# on its own, it survives every replacement.
resource "azurerm_public_ip" "this" {
  name                = var.name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = var.tags
}

# An instance of a Flexible scale set gets no outbound address of its own, and
# the address above only reaches its interface once the first boot has already
# downloaded Docker and called Resource Manager. The NAT gateway is what
# carries that first boot, and every later egress: the instance's downloads,
# the image pulls and whatever the sandboxes reach, which is anywhere.
resource "azurerm_public_ip" "nat" {
  name                = "${var.name}-nat"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = var.tags
}

resource "azurerm_nat_gateway" "this" {
  name                = var.name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku_name            = "Standard"
  tags                = var.tags
}

resource "azurerm_nat_gateway_public_ip_association" "this" {
  nat_gateway_id       = azurerm_nat_gateway.this.id
  public_ip_address_id = azurerm_public_ip.nat.id
}

resource "azurerm_subnet_nat_gateway_association" "this" {
  subnet_id      = azurerm_subnet.this.id
  nat_gateway_id = azurerm_nat_gateway.this.id
}
