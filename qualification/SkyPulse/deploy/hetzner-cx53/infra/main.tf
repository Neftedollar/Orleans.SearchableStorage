locals {
  app_private_ipv4 = "10.42.0.10"
  pg_private_ipv4  = "10.42.0.20"
  internet_cidrs   = ["0.0.0.0/0", "::/0"]

  common_labels = {
    application = "skypulse"
    managed-by  = "opentofu"
    topology    = "single-app-two-node"
  }
}

data "hcloud_ssh_key" "admin" {
  name = var.ssh_key_name
}

resource "hcloud_network" "private" {
  name              = "${var.name_prefix}-private"
  ip_range          = "10.42.0.0/24"
  delete_protection = true
  labels            = local.common_labels

  lifecycle {
    prevent_destroy = true
  }
}

resource "hcloud_network_subnet" "private" {
  network_id   = hcloud_network.private.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = "10.42.0.0/24"
}

resource "hcloud_placement_group" "spread" {
  name   = "${var.name_prefix}-spread"
  type   = "spread"
  labels = local.common_labels
}

resource "hcloud_firewall" "app" {
  name   = "${var.name_prefix}-app"
  labels = local.common_labels

  lifecycle {
    prevent_destroy = true
  }

  rule {
    direction   = "in"
    protocol    = "tcp"
    port        = "22"
    source_ips  = var.admin_ssh_cidrs
    description = "SSH from explicitly approved administrator networks"
  }

  rule {
    direction   = "in"
    protocol    = "icmp"
    source_ips  = local.internet_cidrs
    description = "ICMP and IPv6 path-MTU discovery"
  }

  dynamic "rule" {
    for_each = length(var.public_ui_cidrs) == 0 ? toset([]) : toset(["80", "443"])

    content {
      direction   = "in"
      protocol    = "tcp"
      port        = rule.value
      source_ips  = var.public_ui_cidrs
      description = "Optional SkyPulse UI and TLS ingress"
    }
  }

  rule {
    direction       = "out"
    protocol        = "tcp"
    port            = "53"
    destination_ips = local.internet_cidrs
    description     = "DNS over TCP"
  }

  rule {
    direction       = "out"
    protocol        = "udp"
    port            = "53"
    destination_ips = local.internet_cidrs
    description     = "DNS over UDP"
  }

  rule {
    direction       = "out"
    protocol        = "udp"
    port            = "123"
    destination_ips = local.internet_cidrs
    description     = "NTP"
  }

  rule {
    direction       = "out"
    protocol        = "tcp"
    port            = "80"
    destination_ips = local.internet_cidrs
    description     = "Package and certificate HTTP endpoints"
  }

  rule {
    direction       = "out"
    protocol        = "tcp"
    port            = "443"
    destination_ips = local.internet_cidrs
    description     = "AT Protocol, package, image, and certificate HTTPS endpoints"
  }

  rule {
    direction       = "out"
    protocol        = "tcp"
    port            = "5432"
    destination_ips = ["${local.pg_private_ipv4}/32"]
    description     = "PostgreSQL over the private network only"
  }

  rule {
    direction       = "out"
    protocol        = "icmp"
    destination_ips = local.internet_cidrs
    description     = "ICMP diagnostics and path-MTU discovery"
  }
}

resource "hcloud_firewall" "postgres" {
  name   = "${var.name_prefix}-postgres"
  labels = local.common_labels

  lifecycle {
    prevent_destroy = true
  }

  rule {
    direction   = "in"
    protocol    = "tcp"
    port        = "22"
    source_ips  = var.admin_ssh_cidrs
    description = "SSH from explicitly approved administrator networks"
  }

  rule {
    direction   = "in"
    protocol    = "tcp"
    port        = "5432"
    source_ips  = ["${local.app_private_ipv4}/32"]
    description = "PostgreSQL from the single SkyPulse app server only"
  }

  rule {
    direction   = "in"
    protocol    = "icmp"
    source_ips  = local.internet_cidrs
    description = "ICMP and IPv6 path-MTU discovery"
  }

  rule {
    direction       = "out"
    protocol        = "tcp"
    port            = "53"
    destination_ips = local.internet_cidrs
    description     = "DNS over TCP"
  }

  rule {
    direction       = "out"
    protocol        = "udp"
    port            = "53"
    destination_ips = local.internet_cidrs
    description     = "DNS over UDP"
  }

  rule {
    direction       = "out"
    protocol        = "udp"
    port            = "123"
    destination_ips = local.internet_cidrs
    description     = "NTP"
  }

  rule {
    direction       = "out"
    protocol        = "tcp"
    port            = "80"
    destination_ips = local.internet_cidrs
    description     = "Package HTTP endpoints"
  }

  rule {
    direction       = "out"
    protocol        = "tcp"
    port            = "443"
    destination_ips = local.internet_cidrs
    description     = "Package, image, and encrypted backup endpoints"
  }

  rule {
    direction       = "out"
    protocol        = "icmp"
    destination_ips = local.internet_cidrs
    description     = "ICMP diagnostics and path-MTU discovery"
  }
}

resource "hcloud_server" "app" {
  name               = "${var.name_prefix}-app"
  image              = "ubuntu-24.04"
  server_type        = "cx53"
  location           = var.location
  ssh_keys           = [data.hcloud_ssh_key.admin.id]
  firewall_ids       = [hcloud_firewall.app.id]
  placement_group_id = hcloud_placement_group.spread.id

  backups                  = var.enable_backups
  delete_protection        = true
  rebuild_protection       = true
  shutdown_before_deletion = true

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }

  network {
    subnet_id = hcloud_network_subnet.private.id
    ip        = local.app_private_ipv4
    alias_ips = []
  }

  labels = merge(local.common_labels, {
    role = "app-index-tap"
  })

  depends_on = [hcloud_network_subnet.private]

  lifecycle {
    prevent_destroy = true
  }
}

resource "hcloud_server" "postgres" {
  name               = "${var.name_prefix}-postgres"
  image              = "ubuntu-24.04"
  server_type        = "cx53"
  location           = var.location
  ssh_keys           = [data.hcloud_ssh_key.admin.id]
  firewall_ids       = [hcloud_firewall.postgres.id]
  placement_group_id = hcloud_placement_group.spread.id

  backups                  = var.enable_backups
  delete_protection        = true
  rebuild_protection       = true
  shutdown_before_deletion = true

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }

  network {
    subnet_id = hcloud_network_subnet.private.id
    ip        = local.pg_private_ipv4
    alias_ips = []
  }

  labels = merge(local.common_labels, {
    role = "postgresql"
  })

  depends_on = [hcloud_network_subnet.private]

  lifecycle {
    prevent_destroy = true
  }
}
