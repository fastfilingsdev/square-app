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

## Merge blockers

1. Complete external caller migration and full native route tests. The single
   Sheets batch is not a transaction around preflight reads; manual/API edits
   and ambiguous retries still require an approved concurrency/recovery design.
2. Verify actual database ownership, inherited memberships, serving grants and
   runtime TLS. Apply staged migrations only through a reviewed operator step.
   Never grant direct serving-role writes to bypass permission errors.
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
The legacy single-claim mode is not the approved partial-refund rollout target.

Remaining credential migration, wider authorization findings and all state-sheet
rollouts are separate scope; this draft does not claim the entire foundation is
finished. No customer data, private sheet identifiers or credentials are added.
