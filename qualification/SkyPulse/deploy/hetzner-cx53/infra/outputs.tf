output "network_id" {
  description = "Hetzner ID of the private SkyPulse network."
  value       = hcloud_network.private.id
}

output "app_server" {
  description = "Addresses and identity of the single SkyPulse app/index/TAP server."
  value = {
    id           = hcloud_server.app.id
    name         = hcloud_server.app.name
    public_ipv4  = hcloud_server.app.ipv4_address
    public_ipv6  = hcloud_server.app.ipv6_address
    private_ipv4 = local.app_private_ipv4
  }
}

output "postgres_server" {
  description = "Addresses and identity of the PostgreSQL server. Port 5432 is reachable only from the private app address."
  value = {
    id           = hcloud_server.postgres.id
    name         = hcloud_server.postgres.name
    public_ipv4  = hcloud_server.postgres.ipv4_address
    public_ipv6  = hcloud_server.postgres.ipv6_address
    private_ipv4 = local.pg_private_ipv4
  }
}

output "firewall_ids" {
  description = "Firewall IDs for audit and incident-response checks."
  value = {
    app      = hcloud_firewall.app.id
    postgres = hcloud_firewall.postgres.id
  }
}
