-- Install only during drained maintenance, after 004, by the migration owner.
-- New-order and recovery writers must BOTH use this ledger before activation.
-- Legacy creation functions are revoked from serving roles to prevent bypass.
BEGIN;
CREATE TABLE public.ff_membership_heads (
  provider_scope text NOT NULL CHECK (provider_scope ~ '^[a-zA-Z0-9_-]{1,80}$'),
  customer_hash text NOT NULL CHECK (customer_hash ~ '^[a-f0-9]{64}$'),
  transaction_id text CHECK (transaction_id ~ '^[1-9][0-9]{0,29}$'),
  subscription_id text CHECK (subscription_id ~ '^[1-9][0-9]{0,29}$'),
  PRIMARY KEY(provider_scope, customer_hash),
  CHECK (subscription_id IS NULL OR transaction_id IS NOT NULL)
);
CREATE TABLE public.ff_membership_claims (
  provider_scope text NOT NULL,
  transaction_id text NOT NULL CHECK (transaction_id ~ '^[1-9][0-9]{0,29}$'),
  customer_hash text NOT NULL,
  fingerprint text NOT NULL CHECK (fingerprint ~ '^[a-f0-9]{64}$'),
  previous_subscription_id text CHECK (previous_subscription_id ~ '^[1-9][0-9]{0,29}$'),
  attempt_id uuid NOT NULL,
  subscription_id text CHECK (subscription_id ~ '^[1-9][0-9]{0,29}$'),
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  PRIMARY KEY(provider_scope, transaction_id),
  FOREIGN KEY(provider_scope,customer_hash) REFERENCES public.ff_membership_heads,
  UNIQUE(provider_scope,subscription_id),
  CHECK ((subscription_id IS NULL) = (completed_at IS NULL))
);
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT TRUE, SET TRUE;
REVOKE ALL ON public.ff_membership_heads,public.ff_membership_claims FROM PUBLIC,ff_refund_runtime;
GRANT SELECT,INSERT,UPDATE ON public.ff_membership_heads,public.ff_membership_claims TO ff_subscription_executor;

CREATE FUNCTION public.ff_claim_membership(p_scope text,p_tx text,p_customer text,p_fingerprint text,p_previous text,p_attempt uuid)
RETURNS TABLE(outcome text,subscription_id text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE head public.ff_membership_heads%ROWTYPE; prior public.ff_membership_claims%ROWTYPE; inserted integer;
BEGIN
  IF p_scope IS NULL OR p_tx IS NULL OR p_customer IS NULL OR p_fingerprint IS NULL OR p_attempt IS NULL
    OR p_scope !~ '^[a-zA-Z0-9_-]{1,80}$' OR p_tx !~ '^[1-9][0-9]{0,29}$'
    OR p_customer !~ '^[a-f0-9]{64}$' OR p_fingerprint !~ '^[a-f0-9]{64}$'
    OR (p_previous IS NOT NULL AND p_previous !~ '^[1-9][0-9]{0,29}$') THEN
    RAISE EXCEPTION 'Invalid membership claim';
  END IF;
  -- Preserve old holds/receipts. Never turn a v1 uncertainty into a fresh attempt.
  IF EXISTS(SELECT 1 FROM public.ff_subscription_claims WHERE provider_scope=p_scope AND transaction_id=p_tx) THEN
    RETURN QUERY SELECT 'held'::text,NULL::text; RETURN;
  END IF;
  INSERT INTO public.ff_membership_heads(provider_scope,customer_hash)
    VALUES(p_scope,p_customer) ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT head FROM public.ff_membership_heads
    WHERE provider_scope=p_scope AND customer_hash=p_customer FOR UPDATE;
  SELECT * INTO prior FROM public.ff_membership_claims WHERE provider_scope=p_scope AND transaction_id=p_tx;
  IF FOUND THEN
    IF prior.customer_hash=p_customer AND prior.fingerprint=p_fingerprint
      AND prior.previous_subscription_id IS NOT DISTINCT FROM p_previous AND prior.subscription_id IS NOT NULL THEN
      RETURN QUERY SELECT 'succeeded'::text,prior.subscription_id;
    ELSE RETURN QUERY SELECT 'held'::text,NULL::text; END IF;
    RETURN;
  END IF;
  IF head.transaction_id IS NOT NULL AND
    (head.subscription_id IS NULL OR p_previous IS NULL OR p_previous<>head.subscription_id) THEN
    RETURN QUERY SELECT 'held'::text,NULL::text; RETURN;
  END IF;
  INSERT INTO public.ff_membership_claims(provider_scope,transaction_id,customer_hash,fingerprint,previous_subscription_id,attempt_id)
    VALUES(p_scope,p_tx,p_customer,p_fingerprint,p_previous,p_attempt) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS inserted=ROW_COUNT;
  IF inserted<>1 THEN RETURN QUERY SELECT 'held'::text,NULL::text; RETURN; END IF;
  UPDATE public.ff_membership_heads SET transaction_id=p_tx,subscription_id=NULL
    WHERE provider_scope=p_scope AND customer_hash=p_customer;
  RETURN QUERY SELECT 'claimed'::text,NULL::text;
END $$;

CREATE FUNCTION public.ff_finish_membership(p_scope text,p_tx text,p_customer text,p_fingerprint text,p_previous text,p_attempt uuid,p_subscription text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  IF p_subscription IS NULL OR p_subscription !~ '^[1-9][0-9]{0,29}$' THEN RAISE EXCEPTION 'Invalid membership receipt'; END IF;
  PERFORM 1 FROM public.ff_membership_heads WHERE provider_scope=p_scope AND customer_hash=p_customer
    AND transaction_id=p_tx AND subscription_id IS NULL FOR UPDATE;
  IF NOT FOUND THEN RETURN false; END IF;
  UPDATE public.ff_membership_claims SET subscription_id=p_subscription,completed_at=clock_timestamp()
    WHERE provider_scope=p_scope AND transaction_id=p_tx AND customer_hash=p_customer AND fingerprint=p_fingerprint
      AND previous_subscription_id IS NOT DISTINCT FROM p_previous AND attempt_id=p_attempt AND subscription_id IS NULL;
  IF NOT FOUND THEN RETURN false; END IF;
  UPDATE public.ff_membership_heads SET subscription_id=p_subscription
    WHERE provider_scope=p_scope AND customer_hash=p_customer AND transaction_id=p_tx;
  RETURN true;
END $$;
GRANT CREATE ON SCHEMA public TO ff_subscription_executor;
ALTER FUNCTION public.ff_claim_membership(text,text,text,text,text,uuid) OWNER TO ff_subscription_executor;
ALTER FUNCTION public.ff_finish_membership(text,text,text,text,text,uuid,text) OWNER TO ff_subscription_executor;
REVOKE CREATE ON SCHEMA public FROM ff_subscription_executor;
REVOKE ALL ON FUNCTION public.ff_claim_membership(text,text,text,text,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ff_finish_membership(text,text,text,text,text,uuid,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ff_claim_membership(text,text,text,text,text,uuid) TO ff_refund_runtime;
GRANT EXECUTE ON FUNCTION public.ff_finish_membership(text,text,text,text,text,uuid,text) TO ff_refund_runtime;
REVOKE EXECUTE ON FUNCTION public.ff_claim_subscription(text,text,text,uuid) FROM ff_refund_runtime;
REVOKE EXECUTE ON FUNCTION public.ff_finish_subscription(text,text,text,uuid,text) FROM ff_refund_runtime;
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT FALSE, SET FALSE;
COMMIT;
