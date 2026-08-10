# Third-party material in sse-coordinator

sse-coordinator is MIT licensed and its source was written for this project.
This file records anything that came from elsewhere.

## Dependencies

None, runtime or peer. The published package contains only this code and carries
no third-party notice obligation.

## Prior art

Sharing one connection across browser tabs by electing a leader over
`BroadcastChannel` is a known technique with several published implementations,
`broadcast-channel` (Apache-2.0) among them. The technique is an idea; the
election protocol here was written for this package. No code from those projects
is included.

## Reviewed and cleared

Nothing yet. Findings from `scripts/provenance-check.py` that turn out to be
convergent output rather than copying belong here, with the date and the
reasoning.
