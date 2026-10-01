-- Explicit installation under drained maintenance by the existing schema owner.
BEGIN;
CREATE TABLE public.ff_cancellation_claims (
  provider_scope text NOT NULL CHECK(provider_scope ~ '^[a-zA-Z0-9_-]{1,80}$'),
  subscription_id text NOT NULL CHECK(subscription_id ~ '^[1-9][0-9]{0,29}$'),
  customer_hash text NOT NULL CHECK(customer_hash ~ '^[a-f0-9]{64}$'),
  attempt_id uuid NOT NULL,
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  PRIMARY KEY(provider_scope,subscription_id)
);
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT TRUE, SET TRUE;
REVOKE ALL ON public.ff_cancellation_claims FROM PUBLIC,ff_refund_runtime;
GRANT SELECT,INSERT,UPDATE ON public.ff_cancellation_claims TO ff_subscription_executor;
CREATE FUNCTION public.ff_claim_cancellation(p_scope text,p_sub text,p_customer text,p_attempt uuid)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE inserted integer; prior public.ff_cancellation_claims%ROWTYPE;
BEGIN
  INSERT INTO public.ff_cancellation_claims(provider_scope,subscription_id,customer_hash,attempt_id)
    VALUES(p_scope,p_sub,p_customer,p_attempt) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS inserted=ROW_COUNT;
  IF inserted=1 THEN RETURN 'claimed'; END IF;
  SELECT * INTO STRICT prior FROM public.ff_cancellation_claims WHERE provider_scope=p_scope AND subscription_id=p_sub;
  IF prior.customer_hash=p_customer AND prior.completed_at IS NOT NULL THEN RETURN 'succeeded'; END IF;
  RETURN 'held';
END $$;
CREATE FUNCTION public.ff_finish_cancellation(p_scope text,p_sub text,p_customer text,p_attempt uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  UPDATE public.ff_cancellation_claims SET completed_at=clock_timestamp()
    WHERE provider_scope=p_scope AND subscription_id=p_sub AND customer_hash=p_customer AND attempt_id=p_attempt AND completed_at IS NULL;
  RETURN FOUND;
END $$;
GRANT CREATE ON SCHEMA public TO ff_subscription_executor;
ALTER FUNCTION public.ff_claim_cancellation(text,text,text,uuid) OWNER TO ff_subscription_executor;
ALTER FUNCTION public.ff_finish_cancellation(text,text,text,uuid) OWNER TO ff_subscription_executor;
REVOKE CREATE ON SCHEMA public FROM ff_subscription_executor;
REVOKE ALL ON FUNCTION public.ff_claim_cancellation(text,text,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ff_finish_cancellation(text,text,text,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ff_claim_cancellation(text,text,text,uuid) TO ff_refund_runtime;
GRANT EXECUTE ON FUNCTION public.ff_finish_cancellation(text,text,text,uuid) TO ff_refund_runtime;
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT FALSE, SET FALSE;
COMMIT;
