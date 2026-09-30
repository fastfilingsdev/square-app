-- Explicit operator installation in the existing dedicated ledger database.
-- No startup migration; no refund records or permissions are changed.
-- Installer must be the existing non-superuser migration/schema owner with
-- CREATEROLE; CREATE/USAGE alone cannot grant executor schema permissions.
-- Do not grant schema ownership or CREATEROLE to the serving application.
BEGIN;
CREATE TABLE public.ff_subscription_claims (
  provider_scope text NOT NULL CHECK (provider_scope ~ '^[a-zA-Z0-9_-]{1,80}$'),
  transaction_id text NOT NULL CHECK (transaction_id ~ '^[0-9]{1,30}$'),
  fingerprint text NOT NULL CHECK (fingerprint ~ '^[a-f0-9]{64}$'),
  attempt_id uuid NOT NULL,
  subscription_id text CHECK (subscription_id ~ '^[0-9]{1,30}$'),
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  PRIMARY KEY (provider_scope, transaction_id),
  CHECK ((subscription_id IS NULL) = (completed_at IS NULL))
);
CREATE ROLE ff_subscription_executor NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT TRUE, SET TRUE;
GRANT USAGE ON SCHEMA public TO ff_subscription_executor;
REVOKE ALL ON public.ff_subscription_claims FROM PUBLIC, ff_refund_runtime;
GRANT SELECT, INSERT, UPDATE ON public.ff_subscription_claims TO ff_subscription_executor;

CREATE FUNCTION public.ff_claim_subscription(p_scope text, p_tx text, p_fingerprint text, p_attempt uuid)
RETURNS TABLE(outcome text, subscription_id text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE claimed integer; existing public.ff_subscription_claims%ROWTYPE;
BEGIN
  INSERT INTO public.ff_subscription_claims(provider_scope,transaction_id,fingerprint,attempt_id)
    VALUES(p_scope,p_tx,p_fingerprint,p_attempt) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS claimed = ROW_COUNT;
  IF claimed = 1 THEN RETURN QUERY SELECT 'claimed'::text, NULL::text; RETURN; END IF;
  SELECT * INTO STRICT existing FROM public.ff_subscription_claims
    WHERE provider_scope=p_scope AND transaction_id=p_tx;
  IF existing.fingerprint=p_fingerprint AND existing.subscription_id IS NOT NULL THEN
    RETURN QUERY SELECT 'succeeded'::text, existing.subscription_id;
  ELSE
    RETURN QUERY SELECT 'held'::text, NULL::text;
  END IF;
END $$;
CREATE FUNCTION public.ff_finish_subscription(p_scope text,p_tx text,p_fingerprint text,p_attempt uuid,p_subscription text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF p_subscription IS NULL OR p_subscription !~ '^[0-9]{1,30}$' THEN
    RAISE EXCEPTION 'Invalid subscription receipt';
  END IF;
  UPDATE public.ff_subscription_claims SET subscription_id=p_subscription,completed_at=clock_timestamp()
    WHERE provider_scope=p_scope AND transaction_id=p_tx AND fingerprint=p_fingerprint
      AND attempt_id=p_attempt AND subscription_id IS NULL;
  RETURN FOUND;
END $$;
GRANT CREATE ON SCHEMA public TO ff_subscription_executor;
ALTER FUNCTION public.ff_claim_subscription(text,text,text,uuid) OWNER TO ff_subscription_executor;
ALTER FUNCTION public.ff_finish_subscription(text,text,text,uuid,text) OWNER TO ff_subscription_executor;
REVOKE CREATE ON SCHEMA public FROM ff_subscription_executor;
REVOKE ALL ON FUNCTION public.ff_claim_subscription(text,text,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ff_finish_subscription(text,text,text,uuid,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ff_claim_subscription(text,text,text,uuid) TO ff_refund_runtime;
GRANT EXECUTE ON FUNCTION public.ff_finish_subscription(text,text,text,uuid,text) TO ff_refund_runtime;
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT FALSE, SET FALSE;
COMMIT;
