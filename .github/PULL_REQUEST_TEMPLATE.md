<!--
Thanks for opening a PR. Keep the description honest and tight - reviewers
should be able to predict the diff from this section alone.
-->

## What

<!-- One paragraph: what this PR changes and why now. -->

## How

<!-- Bullet the key implementation choices a reviewer should not have to
re-derive from the diff. Skip if obvious. -->

-

## Verification

<!-- Concrete commands you ran. CI alone is not verification for runtime
behavior; for changes touching the data path, include a stack run. -->

- [ ] `make test` (unit)
- [ ] `make test-mongo` (integration, if sink writer touched)
- [ ] `make test-stack` (integration, if cross-service)
- [ ] At least one chaos scenario in `chaos/scenarios/` (if data-plane behavior touched)
- [ ] `helm lint deploy/helm/pg2mongo-cdc` (if chart touched)

## Compatibility

<!-- Anything an operator upgrading from the previous version needs to
know: topic renames, Mongo schema/index changes, Helm values renames,
new env vars, breaking flag changes. "None" is a valid answer. -->

## Related

<!-- Issue / ADR / runbook links. -->

Closes #
