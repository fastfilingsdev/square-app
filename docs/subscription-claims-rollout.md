# Subscription creation restart protection — staged rollout

## Current candidate: shared membership and cancellation claims

The v1 evidence and instructions below describe migration004, already tested.
The current candidate runtime uses migrations005/006 instead: both new-order
and Google recovery creation share a normalized-email membership head, across
different original payments. Distinct email aliases are not a verified common
identity and must not be described as customer-wide coverage across aliases.
Recovery can advance a completed head only with the exact prior subscription;
the handler independently verifies its terminated status and matching customer.
Cancellation has a separate durable claim keyed by provider/subscription.

The new native suite must pass before installation. Its first attempt was
blocked by local initdb shared-memory permissions before any SQL; previous
migration004 tests do NOT establish migration005/006 correctness. Keep all
activation flags off and maintenance on. Before installation inventory v1 claims,
Google recovery properties and provider receipts, including held operations.
Never reinterpret an old hold as permission to dispatch another payment ID.

Install005 then006 only as the migration owner with explicitly approved
function-only runtime grants. Migration005 revokes the old serving creation
functions. All callers must move together; old code cannot be a live fallback.
Do not grant tables/schema/role administration to the serving login. Read back
actual ACLs and runtime wiring, verify installed Google adapters, then consider
the single approved scheduler. Do not run historical financial jobs as tests.

Rollback: retain every old/new claim, receipt and approval; keep maintenance
and all creation/cancellation gates off. Restoring old source does not authorize
restoring old live writers or widening grants. Resolve uncertain provider
operations separately before any reopening.

## Historical v1 rollout and evidence

This increment adds a dedicated subscription-claim table and two function-only
operations to the existing ledger database. It does not change refund records,
start a scheduler, execute payments, install a migration on startup or reopen traffic.

## Verified evidence

- 494 backend tests passed with networking denied.
- Owner-supplied PostgreSQL18.4 native suite passed: restricted-owner installation,
  rollback after insufficient schema privileges, twelve independent connections
  with one synthetic provider call, changed-order blocking, receipt replay,
  acknowledgement loss, uncertainty, privilege limits, application crash and
  immediate PostgreSQL stop/restart.
- Separate isolated Google helper executions passed receipt persistence,
  uncertain-claim blocking and changed-payment blocking. These do not prove that
  the production Google recovery function has been updated.
- All tests used zero real payments/emails/production mutations.

## Operator installation order

1. Keep application/platform maintenance ON, all financial and job gates OFF,
   daily overlap suspended and auto-deploy OFF. Do not deploy old main.
2. Obtain explicit approval for the new database permissions: the existing
   application runtime may execute only ff_claim_subscription and
   ff_finish_subscription. It receives no table access, schema CREATE, role
   administration or membership in the new non-login ff_subscription_executor.
3. Install migrations/004_subscription_claims.sql as the existing non-superuser
   migration/schema owner with CREATEROLE. CREATE/USAGE alone is insufficient.
   Stop on any error and roll back; do not grant privileges to the serving login
   to work around installation errors. Existing object-name conflicts must be
   investigated, not dropped or overwritten.
4. Pin-deploy the reviewed commit under maintenance and verify the actual serving
   login, expected functions/ACLs, configured ledger wiring and normal health.
   A configured health boolean is not proof of an installed migration.
5. Integrate and verify the separately tested Google Termination C claim helper
   without running live recovery as a test. Keep all existing claim records.
6. Resolve remaining payment-key dependencies, actual installed-caller acceptance
   and cross-writer activation limits before approved automation is enabled.

## Semantics and limits

A claim commits before profile/ARB creation. An uncertain or interrupted attempt
has no automatic expiration/reset/retry. Only a durably saved subscription receipt
can be replayed without a provider call. A changed fingerprint stays blocked.
Provider duplicate responses are not automatically adopted based only on matching
amount/start date. Membership workbook read failures fail closed.

The claim is per provider scope and original payment, not a customer-wide mutex.
Backend new-order numeric invoices and Google RST recovery invoices are disjoint;
that does not prevent two different payments for the same customer. Do not assert
merchant-wide exactly-once creation or enable uncoordinated writers on that basis.

Rollback must keep the claim table and Google claim properties. Reverting to old
code requires live creation gates OFF; never erase holds/receipts to make a retry
possible. The prior release is only a held fallback, not a safe unguarded live writer.
