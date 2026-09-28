# Foundation repairs — draft review only

Do not merge or deploy this branch yet. No migration runs at startup. A draft
pull request is not a sandbox, and this change deliberately disables refunds
without the required server-owned persistence configuration.

## Proposed changes

- Require admin authentication on reporting routes and fixed-loopback calls;
  reject GET sheet mutations, debug configuration disclosure and caller-chosen
  Clover destinations. Return 401 explicitly for unauthorized payment links.
- Preserve filing controls and unrelated review rows; validate schema, formulas,
  merges and customer/merchant identity before a single Google Sheets batch.
  Unknown write outcomes require reconciliation, never blind retries.
- Validate refund amounts in exact cents and approved provider receipt IDs.
  Durable claims precede provider dispatch; uncertain results retain capacity.
- Support stable UUID partial-refund requests, cumulative budgets and a
  function-only serving role. Missing or disabled historical budgets fail closed.
- Add bounded settled/pending provider-history collection and server bootstrap
  wiring, disabled by default. Provider-account/policy drift blocks dispatch.

## Verification and limits

Run backend regressions with outbound networking denied:

```sh
node --require ./test/noNetwork.cjs --test test/*.test.js
```

Private isolated PostgreSQL 18.4 testing separately passed core durability,
partial-budget concurrency/crash recovery, and function-only permission checks.
Those tests do not establish the actual deployment's role-transfer permissions,
pool/TLS connectivity or provider account policy. Private fixtures and reports
are intentionally excluded from this public repository.

An operator-approved dedicated database migration has separately passed a
rollback rehearsal and committed verification under a PostgreSQL 18 non-superuser
administrator. Migration 003 temporarily enables executor SET/INHERIT for the
migration owner, then removes those capabilities before commit. Serving-role
table access is not granted. This does not establish application pool readiness.

`inspectRefundLedgerConnection` in `src/core/refundLedgerPreflight.js` is an
explicit operator-only diagnostic, not a startup hook or HTTP endpoint. It uses
the application pool configuration with one connection and a read-only
transaction to check TLS, durability, role attributes and required privileges.
It never calls the financial functions or reads customer/ledger records, and
sanitizes driver errors. Its mocked regressions are separate from an approved
temporary-container run using pg 8.23.0 and the same pool configuration function:
all 14 read-only checks passed against the dedicated database, with no financial
function calls. The explicit Render-internal TLS mode encrypts traffic but does
not verify certificate identity; this is not verify-full evidence. This probe
does not establish deployed server wiring, provider behavior, or post-rotation
credential authentication. Temporary dependencies were kept outside application
files. Do not expose the database externally merely to run diagnostics.

## Merge blockers

1. Complete external caller migration and full native route tests. The single
   Sheets batch is not a transaction around preflight reads; manual/API edits
   and ambiguous retries still require an approved concurrency/recovery design.
2. Complete deployed application wiring and verify its final credentials and
   configuration. Dedicated schema/grants and the isolated Node connection probe
   are verified, not an end-to-end deployment. Do not replay applied migrations
   or grant direct serving-role writes to bypass permission errors.
3. Reconcile historical refunds before enabling per-charge budgets. Verify
   linked-refund enforcement and exclusion of standalone credits on the actual
   provider account. A snapshot alone does not fence external refund writers.
4. Complete Authorize.Net sandbox response/history testing and production
   caller UUID/receipt handling. Do not use real payments as a substitute.
5. Review rollout and rollback for both API and scheduled workers that track
   main. Keep live refunds disabled until all gates pass; never replay financial
   or historical email jobs to validate deployment.

History wiring requires explicit `FF_REFUND_HISTORY_ENABLED=true`, partial ledger
mode, USD, dedicated ledger configuration and both policy attestations. These
flags record operator verification; setting them does not perform verification.
The serving bootstrap rejects legacy single-claim mode (including an omitted
mode with ledger configuration). It remains an isolated test adapter only and
cannot be used to bypass the approved partial-refund/history requirements.

Remaining credential migration, wider authorization findings and all state-sheet
rollouts are separate scope; this draft does not claim the entire foundation is
finished. No customer data, private sheet identifiers or credentials are added.
