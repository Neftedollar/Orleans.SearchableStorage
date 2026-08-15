variable "name_prefix" {
  description = "Lowercase prefix used for every SkyPulse cloud resource."
  type        = string
  default     = "skypulse"
  nullable    = false

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{0,39}$", var.name_prefix))
    error_message = "name_prefix must be 1-40 lowercase letters, digits, or hyphens and start with a letter or digit."
  }
}

variable "location" {
  description = "Hetzner location for both servers. The allowed locations are in the eu-central network zone."
  type        = string
  default     = "nbg1"
  nullable    = false

  validation {
    condition     = contains(["nbg1", "fsn1", "hel1"], var.location)
    error_message = "location must be nbg1, fsn1, or hel1 so the fixed eu-central private network remains valid."
  }
}

variable "ssh_key_name" {
  description = "Name of an existing SSH public key in the Hetzner project."
  type        = string
  nullable    = false

  validation {
    condition     = length(trimspace(var.ssh_key_name)) > 0 && length(var.ssh_key_name) <= 128
    error_message = "ssh_key_name is required and must name an existing Hetzner SSH key."
  }
}

variable "admin_ssh_cidrs" {
  description = "Non-/0 IPv4 and/or IPv6 CIDRs allowed to connect to TCP/22 on both servers."
  type        = list(string)
  nullable    = false

  validation {
    condition = (
      length(var.admin_ssh_cidrs) > 0
      && alltrue([
        for cidr in var.admin_ssh_cidrs :
        can(cidrhost(trimspace(cidr), 0))
        && try(tonumber(split("/", trimspace(cidr))[1]) > 0, false)
        && !startswith(trimspace(cidr), "192.0.2.")
        && !startswith(trimspace(cidr), "198.51.100.")
        && !startswith(trimspace(cidr), "203.0.113.")
        && !startswith(lower(trimspace(cidr)), "2001:db8:")
      ])
    )
    error_message = "admin_ssh_cidrs must contain at least one real, valid CIDR; /0 and documentation-only networks are rejected."
  }
}

variable "public_ui_cidrs" {
  description = "CIDRs allowed to reach TCP/80 and TCP/443 on the app server. Leave empty to keep the UI private and use an SSH tunnel."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition = alltrue([
      for cidr in var.public_ui_cidrs : can(cidrhost(trimspace(cidr), 0))
    ])
    error_message = "Every public_ui_cidrs entry must be a valid IPv4 or IPv6 CIDR."
  }
}

variable "enable_backups" {
  description = "Enable Hetzner server backups in addition to application-level PostgreSQL backups."
  type        = bool
  default     = true
  nullable    = false
}
