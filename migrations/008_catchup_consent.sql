-- CANDIDATE ONLY: requires restricted installation and native acceptance.
-- Stores authorization, not dispatch authority. Does not enable collection.
BEGIN;
CREATE TABLE public.ff_catchup_quotes (
  ticket_hash text NOT NULL CHECK(ticket_hash ~ '^[a-f0-9]{64}$'),
  quote_id text NOT NULL CHECK(quote_id ~ '^[a-f0-9]{64}$'),
  payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object' AND NOT payload ? 'ticketId'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  accepted_at timestamptz,
  consent_version text CHECK(consent_version='overdue-only-v1'),
  PRIMARY KEY(ticket_hash,quote_id),
  CHECK((accepted_at IS NULL)=(consent_version IS NULL))
);
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT TRUE, SET TRUE;
REVOKE ALL ON public.ff_catchup_quotes FROM PUBLIC,ff_refund_runtime;
GRANT SELECT,INSERT,UPDATE ON public.ff_catchup_quotes TO ff_subscription_executor;

CREATE FUNCTION public.ff_save_catchup_quote(p_ticket text,p_quote text,p_payload jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE prior jsonb; stamp numeric:=extract(epoch FROM clock_timestamp())*1000;
BEGIN
  IF p_ticket IS NULL OR p_ticket !~ '^[a-f0-9]{64}$' OR p_quote IS NULL OR p_quote !~ '^[a-f0-9]{64}$' OR
     p_payload IS NULL OR jsonb_typeof(p_payload)<>'object' OR p_payload ? 'ticketId' OR
     p_payload->>'quoteId' IS DISTINCT FROM p_quote OR p_payload->>'source' IS DISTINCT FROM 'durable-catchup-quote-v1' OR
     p_payload->'historyReconciled' IS DISTINCT FROM 'true'::jsonb OR p_payload->'identityVerified' IS DISTINCT FROM 'true'::jsonb OR
     p_payload->>'currency' IS DISTINCT FROM 'USD' OR
     coalesce(p_payload->>'subscriptionId','') !~ '^[1-9][0-9]{0,29}$' OR
     coalesce(p_payload->>'providerScope','') !~ '^[a-zA-Z0-9_-]{1,80}$' OR
     coalesce(p_payload->>'customerHash','') !~ '^[a-f0-9]{64}$' OR
     coalesce(p_payload->>'recoveryId','') !~ '^[a-f0-9]{64}$' OR
     coalesce(p_payload->>'fingerprint','') !~ '^[a-f0-9]{64}$' OR
     coalesce(p_payload->>'amountMinor','') !~ '^[1-9][0-9]{0,7}$' OR
     coalesce(p_payload->>'issuedAt','') !~ '^[0-9]{13}$' OR coalesce(p_payload->>'expiresAt','') !~ '^[0-9]{13}$' OR
     jsonb_typeof(p_payload->'periods') IS DISTINCT FROM 'array' THEN RETURN false; END IF;
  IF (p_payload->>'issuedAt')::numeric>stamp OR (p_payload->>'expiresAt')::numeric<=stamp OR
     (p_payload->>'expiresAt')::numeric-(p_payload->>'issuedAt')::numeric NOT BETWEEN 1 AND 300000 OR
     jsonb_array_length(p_payload->'periods') NOT BETWEEN 1 AND 120 THEN RETURN false; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(p_ticket,0));
  SELECT payload INTO prior FROM public.ff_catchup_quotes WHERE ticket_hash=p_ticket AND quote_id=p_quote;
  IF FOUND THEN RETURN prior=p_payload; END IF;
  -- A later quote never silently replaces an accepted authorization.
  IF EXISTS(SELECT 1 FROM public.ff_catchup_quotes WHERE ticket_hash=p_ticket AND accepted_at IS NOT NULL) THEN RETURN false; END IF;
  INSERT INTO public.ff_catchup_quotes(ticket_hash,quote_id,payload) VALUES(p_ticket,p_quote,p_payload);
  RETURN true;
END $$;

CREATE FUNCTION public.ff_read_catchup_quote(p_ticket text,p_sub text)
RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
  SELECT payload FROM (SELECT payload FROM public.ff_catchup_quotes WHERE ticket_hash=p_ticket
    ORDER BY created_at DESC,quote_id DESC LIMIT 1) latest
  WHERE payload->>'subscriptionId'=p_sub AND (payload->>'expiresAt')::numeric>extract(epoch FROM clock_timestamp())*1000;
$$;

CREATE FUNCTION public.ff_accept_catchup_quote(p_ticket text,p_quote text,p_binding jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE saved public.ff_catchup_quotes%ROWTYPE; expected jsonb;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(p_ticket,0));
  SELECT * INTO saved FROM public.ff_catchup_quotes WHERE ticket_hash=p_ticket AND quote_id=p_quote FOR UPDATE;
  IF NOT FOUND OR (saved.payload->>'expiresAt')::numeric<=extract(epoch FROM clock_timestamp())*1000 THEN RETURN false; END IF;
  expected=jsonb_build_object('subscriptionId',saved.payload->'subscriptionId','customerHash',saved.payload->'customerHash',
    'recoveryId',saved.payload->'recoveryId','amountMinor',saved.payload->'amountMinor','currency',saved.payload->'currency',
    'periods',saved.payload->'periods','expiresAt',saved.payload->'expiresAt');
  IF p_binding IS DISTINCT FROM expected THEN RETURN false; END IF;
  IF saved.accepted_at IS NOT NULL THEN RETURN true; END IF;
  -- Only the currently displayed newest quote may be accepted. Both functions
  -- take the same ticket lock so replacement cannot race with acceptance.
  IF EXISTS(SELECT 1 FROM public.ff_catchup_quotes WHERE ticket_hash=p_ticket AND
    (accepted_at IS NOT NULL OR (created_at,quote_id)>(saved.created_at,saved.quote_id))) THEN RETURN false; END IF;
  UPDATE public.ff_catchup_quotes SET accepted_at=clock_timestamp(),consent_version='overdue-only-v1'
    WHERE ticket_hash=p_ticket AND quote_id=p_quote;
  RETURN true;
END $$;
GRANT CREATE ON SCHEMA public TO ff_subscription_executor;
ALTER FUNCTION public.ff_save_catchup_quote(text,text,jsonb) OWNER TO ff_subscription_executor;
ALTER FUNCTION public.ff_read_catchup_quote(text,text) OWNER TO ff_subscription_executor;
ALTER FUNCTION public.ff_accept_catchup_quote(text,text,jsonb) OWNER TO ff_subscription_executor;
REVOKE CREATE ON SCHEMA public FROM ff_subscription_executor;
REVOKE ALL ON FUNCTION public.ff_save_catchup_quote(text,text,jsonb),public.ff_read_catchup_quote(text,text),public.ff_accept_catchup_quote(text,text,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ff_save_catchup_quote(text,text,jsonb),public.ff_read_catchup_quote(text,text),public.ff_accept_catchup_quote(text,text,jsonb) TO ff_refund_runtime;
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT FALSE, SET FALSE;
COMMIT;
