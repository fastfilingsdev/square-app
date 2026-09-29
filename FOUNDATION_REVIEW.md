# Foundation repairs — code review checkpoint

Reviewed 29 September 2026. Keep this PR draft: code review is not approval to
merge, deploy, enable financial actions, or replay jobs. Both the API and a
scheduled service track main. No migration runs automatically at startup.

This document replaces the older, contradictory pending-test statements. It
separates verified candidate behavior from remaining production release gates.

## Changes included in this PR

- Reporting routes require admin authentication; sheet mutation uses POST.
  Fixed-loopback callers carry the configured token. Debug credential fragments
  and caller-selected Clover destinations are removed; unauthorized payment-link
  requests return 401. Existing billing Google OAuth access is retained.
- Filing imports validate schema, customer/merchant identity, formulas, merges
  and workflow locks before one scoped Sheets batch. Unrelated review rows,
  filing controls and notes remain outside the write. Ambiguous write outcomes
  require reconciliation, not an automatic retry.
- Refunds use exact cents, linked original-payment IDs, durable ownership claims,
  stable request UUIDs, cumulative partial-refund budgets and function-only DB
  privileges. Uncertain results retain reservations and block further dispatch.
- Settled and pending provider-history collection fails closed on incomplete,
  changing or ambiguous evidence. Serving bootstrap requires explicit partial
  mode; the legacy single-claim adapter is isolated-test-only.
- Paired sales-tax Apps Script candidates provide authenticated import, preserved
  customer metadata and signed timestamp/nonce sync with persistent replay
  rejection. These repository files are not automatically installed in Google.
- The daily cron candidate makes one fixed-endpoint POST per invocation, with
  explicit safe ARB flags, no redirect and no automatic retry.

### Provider-history performance correction

Authenticated read-only verification exposed a sequential-scan deadline failure.
The collector now limits parallel reads to four, stops scheduling after failure,
and drains in-flight reads before rejecting. All batch pages remain enumerated.
Only positive, explicit `settledSuccessfully` summaries in settled batches, with
no credit-reference or conflicting type, avoid redundant details. Every refund,
pending, unknown, missing or contradictory summary still gets details. Exact
reference matching, inventory rechecks, 25-second deadline and 30-second evidence
freshness remain unchanged. No cached/partial balance or stale timestamp fallback.

Six added regressions cover classification, pending records, concurrency/failure
drain and 11,600 settled charges plus a linked refund. A fresh provider retest is
required for this version; local performance evidence alone is not sign-off.

### Prior reviewed correction

Native testing observed a 404 when retrieving a Google ContentService output
URL; the cause was not established. `readGoogleSyncOutput` allows one retry of
the **same read-only GET**, after one second, only for HTTP 404. It enforces the
exact Google HTTPS output origin and `/macros/echo` path, rejects URL credentials
and fragments, and neither forwards the signed body nor follows another redirect.
It never retries the signed POST, other statuses, or transport errors. Persistent
404 remains unconfirmed. Three added regressions exercise these boundaries.

## Verified evidence

### Repository regressions

440 tests passed locally with networking denied, zero failures or skipped tests:

```sh
node --require ./test/noNetwork.cjs --test test/*.test.js
```

These include actual-router caller contracts, identity/schema/formula safeguards,
uncertain writes, refund ownership/budgets, history validation, role restrictions,
signed requests/replay and one-attempt cron behavior. This is local test evidence,
not a GitHub CI pass. No GitHub review submissions, inline review threads or
commit status checks were present at the start of this review. No PR-triggered
workflow runs were returned by the connector.

### Separate native and configuration evidence

- Isolated PostgreSQL 18.4 suites passed durability, concurrent partial-budget
  limits, immediate-stop recovery and function-only serving permissions.
- An approved dedicated migration passed rollback rehearsal and committed role
  checks under a non-superuser administrator. Do not replay these migrations.
- The rotated database credential passed all 14 read-only connection/permission
  checks from the backend host. No financial functions or customer rows were used.
- Saved application ledger settings were subsequently verified, including the
  restricted connection URL and disabled financial/history controls. Save-only
  verification is not proof that the candidate is active in production.
- Backend primary/fallback token alignment and the daily cron token alignment
  were separately verified without publishing their values.
- Native Google tests passed the formula-input safeguard and sync between two
  separate synthetic workbooks, including reversed row order and preserved notes
  and filing stamps.
- The actual Node signed caller passed against a temporary deployed synthetic
  Google endpoint: signed request accepted, ContentService redirect retrieved,
  exact replay rejected. The temporary deployment was archived afterward.
  The final result does not establish whether its 404 retry branch was exercised;
  that branch is proven by the local regressions above.
- A private production billing adapter candidate passed 31 isolated regressions
  and six native synthetic Google cases, including receipt identity, preserved
  email markers and reconciliation on uncertainty. Its install manifest remains
  a separate private rollout artifact, not code shipped by this repository.

Native fixtures, operational reports, workbook IDs and credentials are excluded
from this public repository. These checks made no real payments, refunds or
customer emails. Agent review is not independent reviewer approval or a claim
that the full foundation has shipped.

## Paired caller installation

Install only during an approved, coordinated cutover:

1. Replace the old import with `integrations/sales-tax/02_Sales_Data_Import.gs`.
   Its POST retains query parameters and uses `FF_SYNC_ADMIN_TOKEN` from Script
   Properties. It does not stamp a remembered local customer row.
2. Replace all old state/customer sync definitions (04/05/06) together with
   `04_ConnectionsSync.gs`; do not append duplicate globals. Preserve verified
   header/configuration bindings and backups outside the active source.
3. Replace the webhook with `07_Webhook.gs`. Its dedicated
   `SQ_CUSTOMER_SYNC_SECRET` must match the backend; never reuse billing secrets.
   Keep it disabled until the reviewed production target and caller are ready.
4. Install the separately reviewed private billing adapter with persistent
   request IDs and journal handling; retain the existing token/OAuth access
   contract. Verify the real executor, allowlist and scopes at rollout.

## Existing release gates — not new code-review tasks

1. **Provider verification (completion checklist Step 3).** Finish the remaining
   Authorize.Net response/history and account-policy checks. Initialize budgets
   only after reconciling captured amounts and historical refunds. Staff use
   Refund on the original payment, not unlinked credits. A history snapshot does
   not prevent a concurrent manual/provider refund; actual linked-refund
   enforcement and external-writer controls must be established before enabling.
2. **Paired rollout (Step 5).** Verify backups, rollback, production caller
   installation/authentication, saved-to-runtime configuration activation and
   deployment order. Resolve existing manual/API Sheet-write coordination:
   preflight plus a batch is not a transactional lock; Apps Script locks do not
   fence manual edits or backend writers. Multi-workbook writes can partially
   complete and require reconciliation. Do not enable competing writers until
   their maintenance/recovery controls are established.
3. **Scheduler ownership (Step 5).** The daily cron and embedded 15-minute
   subscription processor are not behaviorally equivalent. Preserve current
   behavior until an owner decision and coverage check establish the authoritative
   scheduler. The one-attempt cron is not cross-invocation idempotency. If adopted,
   keep schedule `30 13,14 * * *`; its script selects 06:30 America/Los_Angeles.
   Inspect platform/operator retry settings; do not test by running a live job.
4. **Production verification (Step 6).** Verify actual installed caller behavior,
   all seven states, billing customer isolation/email markers and stop-work notes
   under the approved rollout plan. Candidate tests do not close this gate.

Keep `FF_BILLING_REFUNDS_DISABLED=true` and `FF_REFUND_HISTORY_ENABLED=false`
until the financial gates are explicitly satisfied. Do not use
`FF_BILLING_REFUNDS_LIVE_ENABLED` as the emergency-disable control. History
enablement additionally requires partial mode, USD, the dedicated ledger and
both policy attestations; setting those flags does not perform verification.

The explicit Render-internal TLS mode encrypts the connection but does not
verify certificate identity. It is not verify-full evidence. Never expose the
database externally or grant direct serving-role table writes to bypass a test.

No production merge, deployment, caller installation, real financial operation
or historical job replay is authorized by this review checkpoint.
