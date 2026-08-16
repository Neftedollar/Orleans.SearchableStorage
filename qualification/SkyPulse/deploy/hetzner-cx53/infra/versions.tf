terraform {
  required_version = ">= 1.8.0, < 2.0.0"

  # Keep both the default-workspace state and any named-workspace state outside
  # the source checkout. The operator must create this root with mode 0700
  # before initialization; see README.md. Do not put credentials in backend
  # configuration or override these paths with an in-repository location.
  backend "local" {
    path          = "/var/lib/skypulse-opentofu/hetzner-cx53/terraform.tfstate"
    workspace_dir = "/var/lib/skypulse-opentofu/hetzner-cx53/workspaces"
  }

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "= 1.66.1"
    }
  }
}

# Supply the token only through the HCLOUD_TOKEN environment variable. Keeping
# it out of configuration prevents it from being copied into tfvars or plans.
provider "hcloud" {}
