# Attribution and license notice

This overlay is a patch against the TAP command in the Indigo repository:

- upstream project: `bluesky-social/indigo`;
- upstream source: <https://github.com/bluesky-social/indigo>;
- pinned revision: `52c38ce3daca2e85a9f70cf052b475506463018e`;
- upstream copyright: Copyright (c) 2022-2026 Bluesky Social PBC,
  `@whyrusleeping`, and contributors.

Indigo is offered under the MIT License or Apache License 2.0, at the
recipient's choice, except where individual files state otherwise. Exact copies
of `LICENSE-DUAL`, `LICENSE-MIT`, and `LICENSE-APACHE` from the pinned revision
are included beside this notice.

The SkyPulse changes are three downstream qualification patches: the
metadata-only protocol overlay, a startup-hardening patch that requires
authenticated WebSocket acknowledgements, and a privacy/logging patch that
enforces closed diagnostics and migration of legacy error text. They are not
endorsed, maintained, or supported by Bluesky Social PBC. AT Protocol, Bluesky,
Indigo, and TAP names are used only to identify interoperability and source
provenance.
