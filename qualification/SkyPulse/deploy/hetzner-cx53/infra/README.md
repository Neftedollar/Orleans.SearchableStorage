# SkyPulse Hetzner infrastructure

This OpenTofu root creates the deliberately single-replica SkyPulse calibration topology:

- one Hetzner `cx53` running the sole application/Memory-index incarnation and patched TAP;
- one Hetzner `cx53` running PostgreSQL;
- one `10.42.0.0/24` private network, with the app at `10.42.0.10` and PostgreSQL at
  `10.42.0.20`;
- separate stateful cloud firewalls for the public interfaces and a spread placement group.

Both servers use the Hetzner `ubuntu-24.04` image. The template contains no cloud-init because
application credentials, PostgreSQL credentials, corpus routes, certificates, and other private
artifacts must not be embedded in provider requests or state.

The servers use ordinary root disks. This module does not configure or prove encryption at rest;
do not copy real private routes, PGDATA, credentials or backups until the local operator has
implemented and restore-tested the separate encrypted-storage prerequisite documented in
`../SECURITY.md`.

## Security defaults

- Public-interface SSH is closed until at least one non-`/0` `admin_ssh_cidrs` entry is supplied.
- PostgreSQL TCP/5432 is accepted only from `10.42.0.10/32` by the mandatory guest firewall over
  the private network.
- UI ports 80 and 443 remain closed when `public_ui_cidrs` is empty. In that mode, use an SSH
  tunnel to the loopback-bound application.
- Public-interface outbound traffic is allowlisted to DNS, NTP, HTTP/HTTPS, and ICMP. The
  mandatory guest firewall separately permits app-to-PostgreSQL private traffic only at
  `10.42.0.20:5432` and blocks other traffic on the private interfaces.
- Server API delete and rebuild protection are enabled. OpenTofu `prevent_destroy` also blocks
  accidental replacement or destruction of both servers and the private network.
- Hetzner backups are enabled by default. They complement rather than replace tested PostgreSQL
  physical backups.

Hetzner Cloud Firewalls do **not** filter traffic carried by Hetzner private Networks. The cloud
rules containing RFC1918 addresses document the intended topology but do not enforce it. Before
starting Docker or accepting SSH on either host, the local deployment agent must install and
reboot-test the role-specific nftables service and systemd dependencies from the parent deployment
bundle. It must also configure PostgreSQL TLS with full certificate verification, OS updates,
Docker, backup retention, and the SkyPulse services described there.

## Prerequisites

1. Install OpenTofu 1.8 or later, but below 2.0.
2. Create an SSH public key in the target Hetzner project and record its exact name.
3. Create a Hetzner API token with the permissions required to manage servers, networks,
   firewalls, placement groups, and SSH-key reads.
4. Decide the actual fixed administrator or VPN CIDRs. The example documentation address will
   not give you access.
5. Obtain a release containing a reviewed `.terraform.lock.hcl`. This extracted bundle does not
   contain one, so initialization, planning, and applying are deliberately blocked until the
   release owner supplies and commits it.

Keep the token out of files:

```bash
export HCLOUD_TOKEN='set-locally-do-not-commit'
```

## Plan and apply

The explicit local backend stores default-workspace state at
`/var/lib/skypulse-opentofu/hetzner-cx53/terraform.tfstate` and named-workspace state below the
adjacent `workspaces/` directory. Create that off-repository root as the account which alone will
run OpenTofu, and use a restrictive umask for every OpenTofu command:

```bash
state_root=/var/lib/skypulse-opentofu/hetzner-cx53
sudo install -d -m 0700 -o "$(id -u)" -g "$(id -g)" \
  "$state_root" "$state_root/workspaces"
umask 077
```

Do not change the backend to an in-repository path. The local backend uses operating-system state
locking, but it is still a single-operator workflow: never run two plans/applies concurrently and
keep an encrypted off-machine copy of every applied state generation. If this root was previously
initialized or any state already exists, stop and perform a separately reviewed state-backup and
`tofu init -migrate-state`; never discard or overwrite the old state to make initialization pass.

The dependency lock is a release artifact, not something to improvise on a deployment host. A
release maintainer must generate it from the origin registry for every approved operator platform,
review the reported signer and checksums, and commit it with the configuration. For the reviewed
Linux/AMD64 operator platform the generation command is:

```bash
tofu providers lock -platform=linux_amd64
```

This repository copy intentionally does not invent that file. Before any initialization, enforce
the local gate and then keep initialization read-only with respect to the lock:

```bash
umask 077
test -f .terraform.lock.hcl && test ! -L .terraform.lock.hcl || {
  echo 'ERROR: reviewed .terraform.lock.hcl is required; do not generate it ad hoc here' >&2
  exit 1
}
git ls-files --error-unmatch -- .terraform.lock.hcl >/dev/null
git diff --quiet HEAD -- .terraform.lock.hcl
repo_root=$(git rev-parse --show-toplevel)
test -z "$(git -C "$repo_root" status --porcelain --untracked-files=all)" || {
  echo 'ERROR: plan/apply requires the exact clean release checkout' >&2
  exit 1
}
for override in override.tf override.tf.json *_override.tf *_override.tf.json; do
  test ! -e "$override" || {
    echo "ERROR: auto-loaded OpenTofu override is forbidden: $override" >&2
    exit 1
  }
done
for candidate in *.tfvars *.tfvars.json; do
  test ! -e "$candidate" || test "$candidate" = terraform.tfvars || {
    echo "ERROR: alternate/auto-loaded variable file is forbidden: $candidate" >&2
    exit 1
  }
done
tofu init -input=false -lockfile=readonly
```

The Git checks deliberately reject an untracked, staged-only or locally modified lock file and
any other tracked/untracked release change. Ignored `terraform.tfvars`, state and `.terraform/`
remain outside this source-integrity assertion and are governed by the protected-state workflow.
Run this workflow only from the exact clean release checkout; a copied or improvised lock is not an
acceptable substitute for the reviewed file in the release commit.

Create the ignored local variable file and replace every example value:

```bash
umask 077
cp terraform.tfvars.example terraform.tfvars
chmod 600 terraform.tfvars
test -f terraform.tfvars && test ! -L terraform.tfvars
test "$(stat -c '%a' terraform.tfvars)" = 600
for candidate in *.tfvars *.tfvars.json; do
  test ! -e "$candidate" || test "$candidate" = terraform.tfvars || {
    echo "ERROR: alternate/auto-loaded variable file is forbidden: $candidate" >&2
    exit 1
  }
done
```

Then format, validate, and save the reviewed plan beside the protected state, not in the checkout:

```bash
umask 077
state_root=/var/lib/skypulse-opentofu/hetzner-cx53
tofu fmt -check -recursive
tofu validate
plan_path="$state_root/skypulse.tfplan"
tofu plan -input=false -lock-timeout=60s -out="$plan_path"
tofu show "$plan_path"
```

Only the local deployment agent should apply the exact reviewed plan:

```bash
umask 077
state_root=/var/lib/skypulse-opentofu/hetzner-cx53
plan_path="$state_root/skypulse.tfplan"
tofu apply -input=false -lock-timeout=60s "$plan_path"
test -f "$state_root/terraform.tfstate"
test "$(stat -c '%a' "$state_root/terraform.tfstate")" = 600
```

After every successful apply, immediately copy the mode-`0600` state to an encrypted off-machine
backup and verify that it can be read before the next change. Never commit `terraform.tfvars`, a
plan, state, `.terraform/`, or `HCLOUD_TOKEN`. If a shared encrypted backend is introduced later,
review its locking, migration, recovery, and credential handling before changing this block; do not
place backend credentials in HCL or CLI arguments.

## Connecting after creation

Read the addresses without exposing any credential:

```bash
tofu output app_server
tofu output postgres_server
```

With no public UI CIDRs, forward the application loopback port after the service bundle is
installed:

```bash
ssh -L 5080:127.0.0.1:5080 root@APP_PUBLIC_IPV4
```

Then open `http://127.0.0.1:5080`. TAP stays on the app host loopback and PostgreSQL uses only the
private address.

## Destructive changes

Normal `tofu destroy`, a forced server replacement, or network removal fails because protection
is deliberate. Do not bypass it during ordinary updates: SkyPulse updates replace containers,
not servers.

If a server must truly be destroyed, first take and verify the required PostgreSQL/application
backups. Then make a separately reviewed change which removes `prevent_destroy` and disables the
corresponding Hetzner delete/rebuild protections. Apply that protection-only change before
planning destruction. Never combine protection removal and destruction in an unreviewed command.
