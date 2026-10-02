-- Candidate read-only extension; does not enable collection or alter approvals.
BEGIN;
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT TRUE, SET TRUE;
CREATE FUNCTION public.ff_read_accepted_catchup_quote(p_ticket text,p_sub text)
RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
  SELECT jsonb_build_object('quote',payload,'acceptedAt',floor(extract(epoch FROM accepted_at)*1000),
    'consentVersion',consent_version)
  FROM public.ff_catchup_quotes
  WHERE ticket_hash=p_ticket AND payload->>'subscriptionId'=p_sub
    AND accepted_at IS NOT NULL AND consent_version='overdue-only-v1'
  ORDER BY accepted_at DESC,quote_id DESC LIMIT 1;
$$;
GRANT CREATE ON SCHEMA public TO ff_subscription_executor;
ALTER FUNCTION public.ff_read_accepted_catchup_quote(text,text) OWNER TO ff_subscription_executor;
REVOKE CREATE ON SCHEMA public FROM ff_subscription_executor;
REVOKE ALL ON FUNCTION public.ff_read_accepted_catchup_quote(text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ff_read_accepted_catchup_quote(text,text) TO ff_refund_runtime;
GRANT ff_subscription_executor TO CURRENT_USER WITH INHERIT FALSE, SET FALSE;
COMMIT;
