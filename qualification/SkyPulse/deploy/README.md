# SkyPulse deployment bundles

This directory contains operator-owned deployment material. It is deliberately
separate from the SkyPulse runtime and qualification code.

- [`hetzner-cx53/`](hetzner-cx53/) is the initial two-host calibration bundle:
  one application host and one PostgreSQL host.

The bundle is fail-closed and contains no credentials, private routes, corpus
observations, live corpus artifacts, Terraform state, or qualification
evidence. Those inputs must be created and reviewed by the local operator.
It also intentionally leaves provider-lock generation, exact external image
digests, encrypted-at-rest host storage and encrypted off-host recovery to
explicit local gates; it must not be treated as a one-command production deploy.
