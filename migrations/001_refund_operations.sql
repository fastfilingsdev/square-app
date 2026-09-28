-- Apply explicitly to an isolated database first; never run at server startup.
-- Do not use IF NOT EXISTS: an unexpected existing schema must fail review.
BEGIN;
CREATE TABLE ff_refund_operations (
  provider_scope text NOT NULL CHECK (provider_scope ~ '^[a-zA-Z0-9_-]{1,80}$'),
  original_transaction_id text NOT NULL CHECK (original_transaction_id ~ '^[1-9][0-9]{0,29}$'),
  amount numeric(12,2) NOT NULL CHECK (amount > 0),
  attempt_id uuid NOT NULL UNIQUE,
  state text NOT NULL CHECK (state IN ('dispatching', 'succeeded', 'needs_reconciliation')),
  provider_refund_id text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (provider_scope, original_transaction_id),
  CHECK ((state = 'succeeded' AND provider_refund_id IS NOT NULL
          AND provider_refund_id ~ '^[1-9][0-9]{0,29}$')
      OR (state <> 'succeeded' AND provider_refund_id IS NULL))
);
COMMENT ON TABLE ff_refund_operations IS
  'No automatic expiry, deletion or retry. Additional refunds against the same original charge require reconciled, separately reviewed handling.';
COMMIT;
