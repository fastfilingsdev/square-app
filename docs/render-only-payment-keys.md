# Render-only payment keys — installed migration and acceptance

## 1 October verified checkpoint

Runtime b82fbf6 is pinned on Render (deployment dep-dav2ggo473hc73d8ovgg).
All nine intended production Google source replacements were installed with
exact persisted readbacks; eleven other source files were preserved. Active
source has no legacy payment credentials or direct provider transport. Existing
recovery journals/receipts remain intact. Historical frozen source versions may
still contain old credentials: retirement and owner-coordinated rotation remain
an explicit exposure-cleanup follow-up, not a completed action.

Approved database migrations005/006 and the four function-only serving grants
passed production installation/ACL checks. The native PostgreSQL membership and
cancellation suite passed, including concurrency and restart durability. The
offline repository suite passes536 tests. Actual seven-operation provider reads,
Google OAuth batch reporting and invalid-token rejection passed. Installed
Billing/schema/ledger readiness, no-dispatch request rejection and signed Google
connection acceptance passed. General refund caller and signed sync are enabled;
the previously owner-approved refund independently reached final settlement.
No additional real-money acceptance test was induced.

Maintenance is off; the approved15-minute new-orders scheduler completed its
first cycle with zero proposed/customer/subscription changes. Duplicate daily
cron stays suspended and auto-deploy stays off. Do not deploy latest main.
Separate recovery/background-job gates are not all restored yet; foundation
closure and normal scheduled state-sync follow-up remain open.

Production Google recovery preview checked953 transactions, planned zero creates,
and wrote/sent nothing. A discovered text-versus-checkbox mismatch in the stop-work
writer was repaired and tested in native isolated Sheets. Read-only downstream
preflight passed44 sheets; PA semiannual's empty T7:X7 merge was then removed with
exact surrounding cell metadata preserved, and its narrow preflight passed.
These45 successful preflights are not historical-job replay or a live dispatch.

The sections below are the original candidate design/history. Their statements
that installation/native checks had not happened are superseded by this dated
checkpoint; their transport, privacy and durable-hold requirements still apply.

Owner selected Render-only keys on 1 October 2026. Do not move Authorize.Net
login, transaction or signature keys into Google Script Properties. Do not
rotate keys as a side effect of this migration.

## Implemented locally, not installed

`src/features/googlePaymentReads/gateway.js` accepts seven explicitly validated
read operations only. OAuth must resolve to verified returns@fastfilings.com;
there is no admin-token bypass. Provider credentials come only from Render
environment variables. Transport uses a fixed provider endpoint, no redirects,
no retry, time/body/response limits and at most eight concurrent requests.
Errors do not return or log credential-bearing Axios objects. Responses remove
credentials and sensitive payment fields and mask account/card numbers.

The candidate mounts read and dedicated mutation routers under `/google-payments`
behind the existing maintenance middleware, with no maintenance exception.
FF_GOOGLE_PAYMENT_READS_ENABLED must equal `true`; absence fails closed.
Recovery and cancellation have separate default-off gates. Enablement is a
separate reviewed cutover action, not part of installing this candidate.

`docs/google-payment-read-adapter.gs` sends a fresh ScriptApp OAuth token to the
fixed Render endpoint. It has no provider fallback or stored payment secret.
Batch reads use groups of four, preserve response order and retain per-item
errors without retry. This is an adapter, not yet a replacement for each actual
Google consumer. Existing consumer field requirements must be checked before
installation, especially payment-field redaction and date/paging formats.

Exact production reporting source uses string booleans for sorting. The adapter
now normalizes only those known values without mutating the original object.
`google-payment-read-replacements.gs` contains candidate replacements for the
two read transports, subscription-detail batching and webhook transaction lookup.
They reject old embedded credentials rather than forwarding them. Request
builders must be rewritten before installation. Twelve isolated read-bridge and
caller tests pass; this does not include production installation or native Google
execution of the new adapter.

Dedicated recovery and cancellation handlers now verify a fixed workbook row,
provider identity and operation evidence. Recovery shares the normalized-email
membership ledger with new orders, including across different payment IDs.
Cancellation has its own durable one-attempt ledger. Missing/uncertain receipts
remain held, never automatically released or retried. Each mutation endpoint
admits at most four concurrent requests before OAuth verification. Neither
handler writes sheets or sends email. Keys remain exclusively in Render.

The exact current 20-file Google source inventory was transformed privately:
nine files change, eleven remain identical, and all twenty plus the shared
adapter parse successfully. Named legacy key references and direct provider
transports are absent from the transformed sources. Existing local recovery
journals remain intact. These are in-memory candidates, not installed scripts;
syntax checks alone are not Google execution or financial acceptance evidence.
The source preparer fails on unexpected functions/call sites rather than guessing.

## Still required before removal of Google keys/reopening

1. Adapt and test all seven production read consumers against exact source and
   Google response behavior. Preserve list/detail pagination and ID matching.
2. Natively verify and install the dedicated mutation handlers and migrations
   005/006, with the approved function-only privileges. The isolated native
   membership/cancellation suite is prepared but has NOT passed: the local
   sandbox blocked initdb before SQL. Inventory all older database and Google
   claims first; do not release or discard uncertain historical holds.
3. Migrate webhook signature verification to Render and account for old frozen
   Google deployments/other callers. Existing provider registration at Render
   alone does not establish that all old Google callers are gone.
4. Review/publish, install disabled, verify installed-caller acceptance, then
   remove hardcoded key dependencies. Historical source versions can still
   retain keys; owner-coordinated rotation/retirement is needed for final
   credential exposure cleanup, not an automatic action of this candidate.
5. Only after those gates: approved reopening and one 15-minute scheduler.

No financial operation, provider request, credential change, database migration,
Google install, deploy or scheduler activation is part of these local tests.
