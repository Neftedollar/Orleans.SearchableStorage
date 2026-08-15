# Security policy

This repository has no supported release yet. Report vulnerabilities privately through GitHub's
security-advisory flow for this repository. Do not place credentials, private routing data,
personal identifiers, source content, or exploitable details in a public issue.

Qualification infrastructure must use deployment secret stores. The private DID journal and
routing artifacts require encrypted, access-controlled storage with finite retention. TAP and
application logs are restricted metadata sinks and must be reviewed and sanitized before any
public evidence release. Raw record bodies, post/profile text, handles, media, and content-derived
diagnostics are outside the qualification storage contract.

The patched TAP source retains Indigo's upstream dual-license terms separately under
`provision/tap/`.
