-- Candidate only. Install under approved, drained cutover after 004-006.
-- No enabling flags, historical recovery replay or approval/hold release here.
BEGIN;
CREATE TABLE public.ff_catchup_operations (
  provider_scope text NOT NULL CHECK(provider_scope ~ '^[a-zA-Z0-9_-]{1,80}$'),
  subscription_id text NOT NULL CHECK(subscription_id ~ '^[1-9][0-9]{0,29}$'),
  customer_hash text NOT NULL CHECK(customer_hash ~ '^[a-f0-9]{64}$'),
  recovery_id text NOT NULL CHECK(recovery_id ~ '^[a-f0-9]{64}$'),
  periods text[] NOT NULL CHECK(cardinality(periods) BETWEEN 1 AND 120),
  amount_minor bigint NOT NULL CHECK(amount_minor BETWEEN 1 AND 99999999),
  currency text NOT NULL CHECK(currency='USD'),
  fingerprint text NOT NULL CHECK(fingerprint ~ '^[a-f0-9]{64}$'),
  attempt_id uuid NOT NULL,
  state text NOT NULL DEFAULT 'dispatching' CHECK(state IN ('dispatching','succeeded','declined')),
  transaction_id text CHECK(transaction_id ~ '^[1-9][0-9]{0,29}$'),
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  PRIMARY KEY(provider_scope,subscription_id,recovery_id),
  UNIQUE(provider_scope,transaction_id),
  CHECK((state='dispatching' AND transaction_id IS NULL AND completed_at IS NULL) OR
        (state IN ('succeeded','declined') AND transaction_id IS NOT NULL AND completed_at IS NOT NULL))
);
CREATE TABLE public.ff_catchup_periods (
  provider_scope text NOT NULL,
  subscription_id text NOT NULL,
  due_date date NOT NULL,
  recovery_id text NOT NULL,
  PRIMARY KEY(provider_scope,subscription_id,due_date),
  FOREIGN KEY(provider_scope,subscription_id,recovery_id) REFERENCES public.ff_catchup_operations
);
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT TRUE, SET TRUE;
REVOKE ALL ON public.ff_catchup_operations,public.ff_catchup_periods FROM PUBLIC,ff_refund_runtime;
GRANT SELECT,INSERT,UPDATE ON public.ff_catchup_operations TO ff_subscription_executor;
GRANT SELECT,INSERT ON public.ff_catchup_periods TO ff_subscription_executor;

CREATE FUNCTION public.ff_claim_catchup(p_scope text,p_sub text,p_customer text,p_recovery text,p_periods text[],
  p_amount bigint,p_currency text,p_fingerprint text,p_attempt uuid)
RETURNS TABLE(outcome text,attempt_id uuid,transaction_id text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE prior public.ff_catchup_operations%ROWTYPE; item text;
BEGIN
  IF p_scope IS NULL OR p_scope !~ '^[a-zA-Z0-9_-]{1,80}$' OR
     p_sub IS NULL OR p_sub !~ '^[1-9][0-9]{0,29}$' OR
     p_customer IS NULL OR p_customer !~ '^[a-f0-9]{64}$' OR
     p_recovery IS NULL OR p_recovery !~ '^[a-f0-9]{64}$' OR
     p_fingerprint IS NULL OR p_fingerprint !~ '^[a-f0-9]{64}$' OR p_attempt IS NULL OR
     p_amount IS NULL OR p_amount NOT BETWEEN 1 AND 99999999 OR p_currency IS DISTINCT FROM 'USD' OR
     p_periods IS NULL OR cardinality(p_periods) NOT BETWEEN 1 AND 120 OR
     array_ndims(p_periods)<>1 OR cardinality(p_periods)<>(SELECT count(DISTINCT value) FROM unnest(p_periods) value) THEN
    RAISE EXCEPTION 'Invalid catch-up claim';
  END IF;
  FOREACH item IN ARRAY p_periods LOOP
    IF item !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' OR to_char(item::date,'YYYY-MM-DD')<>item THEN
      RAISE EXCEPTION 'Invalid catch-up period';
    END IF;
  END LOOP;
  -- Serialize the entire subscription, including differently grouped periods.
  -- Hash collisions only serialize unrelated work; they cannot admit a replay.
  PERFORM pg_advisory_xact_lock(hashtextextended(p_scope||chr(31)||p_sub,0));
  SELECT * INTO prior FROM public.ff_catchup_operations
    WHERE provider_scope=p_scope AND subscription_id=p_sub AND recovery_id=p_recovery;
  IF FOUND THEN
    IF prior.customer_hash=p_customer AND prior.periods=p_periods AND prior.amount_minor=p_amount AND
       prior.currency=p_currency AND prior.fingerprint=p_fingerprint AND prior.state='succeeded' THEN
      RETURN QUERY SELECT 'succeeded'::text,NULL::uuid,prior.transaction_id;
    ELSE RETURN QUERY SELECT 'held'::text,NULL::uuid,NULL::text; END IF;
    RETURN;
  END IF;
  IF EXISTS(SELECT 1 FROM public.ff_catchup_operations WHERE provider_scope=p_scope AND subscription_id=p_sub AND state='dispatching') OR
     EXISTS(SELECT 1 FROM public.ff_catchup_periods WHERE provider_scope=p_scope AND subscription_id=p_sub AND due_date=ANY(p_periods::date[])) THEN
    RETURN QUERY SELECT 'held'::text,NULL::uuid,NULL::text; RETURN;
  END IF;
  INSERT INTO public.ff_catchup_operations(provider_scope,subscription_id,customer_hash,recovery_id,periods,
    amount_minor,currency,fingerprint,attempt_id)
    VALUES(p_scope,p_sub,p_customer,p_recovery,p_periods,p_amount,p_currency,p_fingerprint,p_attempt);
  INSERT INTO public.ff_catchup_periods(provider_scope,subscription_id,due_date,recovery_id)
    SELECT p_scope,p_sub,value::date,p_recovery FROM unnest(p_periods) value;
  RETURN QUERY SELECT 'claimed'::text,p_attempt,NULL::text;
END $$;

CREATE FUNCTION public.ff_finish_catchup(p_scope text,p_sub text,p_customer text,p_recovery text,p_attempt uuid,p_state text,p_tx text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  IF p_state IS NULL OR p_state NOT IN ('succeeded','declined') OR p_tx IS NULL OR p_tx !~ '^[1-9][0-9]{0,29}$' THEN
    RAISE EXCEPTION 'Invalid catch-up receipt';
  END IF;
  UPDATE public.ff_catchup_operations SET state=p_state,transaction_id=p_tx,completed_at=clock_timestamp()
    WHERE provider_scope=p_scope AND subscription_id=p_sub AND customer_hash=p_customer AND recovery_id=p_recovery
      AND attempt_id=p_attempt AND state='dispatching';
  RETURN FOUND;
END $$;
GRANT CREATE ON SCHEMA public TO ff_subscription_executor;
ALTER FUNCTION public.ff_claim_catchup(text,text,text,text,text[],bigint,text,text,uuid) OWNER TO ff_subscription_executor;
ALTER FUNCTION public.ff_finish_catchup(text,text,text,text,uuid,text,text) OWNER TO ff_subscription_executor;
REVOKE CREATE ON SCHEMA public FROM ff_subscription_executor;
REVOKE ALL ON FUNCTION public.ff_claim_catchup(text,text,text,text,text[],bigint,text,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ff_finish_catchup(text,text,text,text,uuid,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ff_claim_catchup(text,text,text,text,text[],bigint,text,text,uuid) TO ff_refund_runtime;
GRANT EXECUTE ON FUNCTION public.ff_finish_catchup(text,text,text,text,uuid,text,text) TO ff_refund_runtime;

-- Exactly-once dispatch intent, not a claim of exactly-once inbox delivery.
-- A lost sender acknowledgement remains claimed until an operator reconciles it.
CREATE TABLE public.ff_catchup_confirmations (
  provider_scope text NOT NULL,
  subscription_id text NOT NULL,
  recovery_id text NOT NULL,
  customer_hash text NOT NULL CHECK(customer_hash ~ '^[a-f0-9]{64}$'),
  outcome_code text NOT NULL CHECK(outcome_code IN ('active','activation_pending','declined','payment_pending')),
  message_hash text NOT NULL CHECK(message_hash ~ '^[a-f0-9]{64}$'),
  attempt_id uuid NOT NULL,
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  receipt_hash text CHECK(receipt_hash ~ '^[a-f0-9]{64}$'),
  completed_at timestamptz,
  PRIMARY KEY(provider_scope,subscription_id,recovery_id,outcome_code),
  FOREIGN KEY(provider_scope,subscription_id,recovery_id) REFERENCES public.ff_catchup_operations,
  CHECK((receipt_hash IS NULL)=(completed_at IS NULL))
);
REVOKE ALL ON public.ff_catchup_confirmations FROM PUBLIC,ff_refund_runtime;
GRANT SELECT,INSERT,UPDATE ON public.ff_catchup_confirmations TO ff_subscription_executor;
CREATE FUNCTION public.ff_claim_catchup_confirmation(p_scope text,p_sub text,p_recovery text,p_customer text,p_code text,p_hash text,p_attempt uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE source public.ff_catchup_operations%ROWTYPE;
BEGIN
  SELECT * INTO source FROM public.ff_catchup_operations WHERE provider_scope=p_scope AND subscription_id=p_sub
    AND recovery_id=p_recovery AND customer_hash=p_customer;
  IF NOT FOUND OR p_code IS NULL OR p_code NOT IN ('active','activation_pending','declined','payment_pending') OR
     p_hash IS NULL OR p_hash !~ '^[a-f0-9]{64}$' OR p_attempt IS NULL THEN RETURN false; END IF;
  IF (p_code IN ('active','activation_pending') AND source.state<>'succeeded') OR
     (p_code='declined' AND source.state<>'declined') OR
     (p_code='payment_pending' AND source.state<>'dispatching') THEN RETURN false; END IF;
  INSERT INTO public.ff_catchup_confirmations(provider_scope,subscription_id,recovery_id,customer_hash,outcome_code,message_hash,attempt_id)
    VALUES(p_scope,p_sub,p_recovery,p_customer,p_code,p_hash,p_attempt) ON CONFLICT DO NOTHING;
  RETURN FOUND;
END $$;
CREATE FUNCTION public.ff_finish_catchup_confirmation(p_scope text,p_sub text,p_recovery text,p_customer text,p_code text,p_hash text,p_attempt uuid,p_receipt text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  IF p_receipt IS NULL OR p_receipt !~ '^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'Invalid confirmation receipt'; END IF;
  UPDATE public.ff_catchup_confirmations SET receipt_hash=p_receipt,completed_at=clock_timestamp()
    WHERE provider_scope=p_scope AND subscription_id=p_sub AND recovery_id=p_recovery AND customer_hash=p_customer
      AND outcome_code=p_code AND message_hash=p_hash AND attempt_id=p_attempt AND completed_at IS NULL;
  RETURN FOUND;
END $$;
GRANT CREATE ON SCHEMA public TO ff_subscription_executor;
ALTER FUNCTION public.ff_claim_catchup_confirmation(text,text,text,text,text,text,uuid) OWNER TO ff_subscription_executor;
ALTER FUNCTION public.ff_finish_catchup_confirmation(text,text,text,text,text,text,uuid,text) OWNER TO ff_subscription_executor;
REVOKE CREATE ON SCHEMA public FROM ff_subscription_executor;
REVOKE ALL ON FUNCTION public.ff_claim_catchup_confirmation(text,text,text,text,text,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ff_finish_catchup_confirmation(text,text,text,text,text,text,uuid,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ff_claim_catchup_confirmation(text,text,text,text,text,text,uuid) TO ff_refund_runtime;
GRANT EXECUTE ON FUNCTION public.ff_finish_catchup_confirmation(text,text,text,text,text,text,uuid,text) TO ff_refund_runtime;
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT FALSE, SET FALSE;
COMMIT;
