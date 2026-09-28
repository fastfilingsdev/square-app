-- Staged only. Apply after 001 to an isolated database and native-test first.
-- No automatic initialization: an authorized reconciliation process must verify
-- captured amount/currency, historical refunds, and control of external writers.
-- The serving application must NOT have direct table mutation privileges.
BEGIN;
CREATE TABLE ff_refund_charge_budgets (
  provider_scope text NOT NULL CHECK (provider_scope ~ '^[a-zA-Z0-9_-]{1,80}$'),
  original_transaction_id text NOT NULL CHECK (original_transaction_id ~ '^[1-9][0-9]{0,29}$'),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  original_minor bigint NOT NULL CHECK (original_minor > 0 AND original_minor <= 999999999999),
  consumed_minor bigint NOT NULL CHECK (consumed_minor >= 0 AND consumed_minor <= original_minor),
  enabled boolean NOT NULL DEFAULT false,
  PRIMARY KEY (provider_scope, original_transaction_id)
);
CREATE TABLE ff_partial_refund_operations (
  provider_scope text NOT NULL,
  original_transaction_id text NOT NULL,
  request_id uuid NOT NULL,
  attempt_id uuid NOT NULL UNIQUE,
  amount_minor bigint NOT NULL CHECK (amount_minor > 0 AND amount_minor <= 999999999999),
  state text NOT NULL CHECK (state IN ('dispatching', 'succeeded', 'needs_reconciliation')),
  provider_refund_id text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (provider_scope, request_id),
  FOREIGN KEY (provider_scope, original_transaction_id)
    REFERENCES ff_refund_charge_budgets (provider_scope, original_transaction_id),
  UNIQUE (provider_scope, provider_refund_id),
  CHECK ((state = 'succeeded' AND provider_refund_id IS NOT NULL
    AND provider_refund_id ~ '^[1-9][0-9]{0,29}$') OR
    (state <> 'succeeded' AND provider_refund_id IS NULL))
);
CREATE INDEX ff_partial_refund_charge_idx ON ff_partial_refund_operations
  (provider_scope, original_transaction_id);

CREATE FUNCTION ff_claim_partial_refund(
  p_scope text, p_transaction text, p_request uuid, p_amount bigint,
  p_currency text, p_attempt uuid
) RETURNS TABLE(attempt_id uuid) LANGUAGE plpgsql
SET search_path = pg_catalog, public AS $$
DECLARE charge public.ff_refund_charge_budgets%ROWTYPE;
BEGIN
  IF p_amount IS NULL OR p_amount <= 0 OR p_amount > 999999999999
      OR p_request IS NULL OR p_attempt IS NULL OR p_currency IS NULL THEN
    RAISE EXCEPTION 'Invalid refund reservation';
  END IF;
  -- Lock one original payment, including different request IDs and amounts.
  SELECT * INTO charge FROM public.ff_refund_charge_budgets b
    WHERE b.provider_scope = p_scope AND b.original_transaction_id = p_transaction
    FOR UPDATE;
  IF NOT FOUND OR NOT charge.enabled OR charge.currency <> p_currency THEN RETURN; END IF;
  -- Prior-version operations must be explicitly reconciled before migration.
  IF EXISTS (SELECT 1 FROM public.ff_refund_operations o
      WHERE o.provider_scope = p_scope AND o.original_transaction_id = p_transaction)
    OR EXISTS (SELECT 1 FROM public.ff_partial_refund_operations o
      WHERE o.provider_scope = p_scope AND o.request_id = p_request)
    OR EXISTS (SELECT 1 FROM public.ff_partial_refund_operations o
      WHERE o.provider_scope = p_scope AND o.original_transaction_id = p_transaction
      AND o.state <> 'succeeded') THEN RETURN; END IF;
  IF p_amount > charge.original_minor - charge.consumed_minor THEN RETURN; END IF;
  UPDATE public.ff_refund_charge_budgets b SET consumed_minor = consumed_minor + p_amount
    WHERE b.provider_scope = p_scope AND b.original_transaction_id = p_transaction;
  INSERT INTO public.ff_partial_refund_operations
    (provider_scope, original_transaction_id, request_id, attempt_id, amount_minor, state)
    VALUES (p_scope, p_transaction, p_request, p_attempt, p_amount, 'dispatching');
  RETURN QUERY SELECT p_attempt;
END;
$$;

CREATE FUNCTION ff_finish_partial_refund(
  p_scope text, p_transaction text, p_request uuid, p_amount bigint,
  p_attempt uuid, p_state text, p_receipt text
) RETURNS TABLE(attempt_id uuid) LANGUAGE plpgsql
SET search_path = pg_catalog, public AS $$
BEGIN
  IF p_state IS NULL OR p_state NOT IN ('succeeded', 'needs_reconciliation') THEN
    RAISE EXCEPTION 'Invalid refund completion';
  END IF;
  PERFORM 1 FROM public.ff_refund_charge_budgets b
    WHERE b.provider_scope = p_scope AND b.original_transaction_id = p_transaction FOR UPDATE;
  RETURN QUERY UPDATE public.ff_partial_refund_operations o
    SET state = p_state, provider_refund_id = p_receipt, updated_at = clock_timestamp()
    WHERE o.provider_scope = p_scope AND o.original_transaction_id = p_transaction
      AND o.request_id = p_request AND o.attempt_id = p_attempt
      AND o.amount_minor = p_amount AND o.state = 'dispatching'
    RETURNING o.attempt_id;
  -- Success does not release capacity; uncertain results retain it as well.
END;
$$;
REVOKE ALL ON FUNCTION ff_claim_partial_refund(text,text,uuid,bigint,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION ff_finish_partial_refund(text,text,uuid,bigint,uuid,text,text) FROM PUBLIC;
-- Role ownership/grants and reconciliation initialization are separate reviewed
-- rollout steps. No SECURITY DEFINER shortcut or public dispatch permissions.
COMMIT;
