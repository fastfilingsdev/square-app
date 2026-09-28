-- STAGED: dedicated ledger database only; explicit operator migration, never startup.
-- Requires a migration administrator able to create roles and transfer ownership.
-- Existing role names deliberately fail: do not silently reuse unknown privileges.
-- Native privilege tests and deployment review are required before production use.
BEGIN;
CREATE ROLE ff_refund_executor NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE ff_refund_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO ff_refund_executor, ff_refund_runtime;
REVOKE ALL ON public.ff_refund_operations, public.ff_refund_charge_budgets,
  public.ff_partial_refund_operations FROM PUBLIC, ff_refund_runtime;
GRANT SELECT ON public.ff_refund_operations TO ff_refund_executor;
GRANT SELECT, UPDATE ON public.ff_refund_charge_budgets TO ff_refund_executor;
GRANT SELECT, INSERT, UPDATE ON public.ff_partial_refund_operations TO ff_refund_executor;

-- All table references in these functions are schema-qualified. Exclude public
-- from lookup and put pg_temp last so callers cannot shadow trusted objects.
ALTER FUNCTION public.ff_claim_partial_refund(text,text,uuid,bigint,text,uuid)
  SET search_path = pg_catalog, pg_temp;
ALTER FUNCTION public.ff_finish_partial_refund(text,text,uuid,bigint,uuid,text,text)
  SET search_path = pg_catalog, pg_temp;
ALTER FUNCTION public.ff_claim_partial_refund(text,text,uuid,bigint,text,uuid) SECURITY DEFINER;
ALTER FUNCTION public.ff_finish_partial_refund(text,text,uuid,bigint,uuid,text,text) SECURITY DEFINER;

-- Temporary CREATE allows ownership transfer; it is removed before commit.
GRANT CREATE ON SCHEMA public TO ff_refund_executor;
ALTER FUNCTION public.ff_claim_partial_refund(text,text,uuid,bigint,text,uuid) OWNER TO ff_refund_executor;
ALTER FUNCTION public.ff_finish_partial_refund(text,text,uuid,bigint,uuid,text,text) OWNER TO ff_refund_executor;
REVOKE CREATE ON SCHEMA public FROM ff_refund_executor;
REVOKE ALL ON FUNCTION public.ff_claim_partial_refund(text,text,uuid,bigint,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ff_finish_partial_refund(text,text,uuid,bigint,uuid,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ff_claim_partial_refund(text,text,uuid,bigint,text,uuid) TO ff_refund_runtime;
GRANT EXECUTE ON FUNCTION public.ff_finish_partial_refund(text,text,uuid,bigint,uuid,text,text) TO ff_refund_runtime;
-- No login/secret or membership is provisioned here. A reviewed serving login
-- may inherit ONLY runtime, never executor/migration roles or table ownership.
-- Historical budget seeding stays outside serving privileges; no reset API.
COMMIT;
