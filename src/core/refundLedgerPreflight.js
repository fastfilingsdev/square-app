// Explicit operator diagnostic only. Never imported by server startup or routes.
// Uses the application's pool configuration but cannot claim/finish a refund.
const { ledgerPoolConfig } = require('./refundLedgerRuntime');

const INSPECT = `SELECT
  current_user = session_user AS original_role,
  NOT (r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls) AS restricted_role,
  NOT pg_is_in_recovery() AS primary_server,
  current_setting('fsync') = 'on' AS durable_storage,
  current_setting('synchronous_commit') = 'on' AS durable_commit,
  current_setting('transaction_read_only') = 'on' AS read_only_probe,
  EXISTS (SELECT 1 FROM pg_catalog.pg_stat_ssl WHERE pid = pg_backend_pid()
    AND ssl AND version IN ('TLSv1.2','TLSv1.3')) AS encrypted_connection,
  NOT has_schema_privilege(current_user,'public','CREATE') AS no_schema_create,
  NOT pg_has_role(current_user,'ff_refund_executor','MEMBER') AS no_executor_membership,
  NOT has_table_privilege(current_user,'public.ff_refund_operations','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS no_legacy_table_access,
  NOT has_table_privilege(current_user,'public.ff_refund_charge_budgets','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS no_budget_table_access,
  NOT has_table_privilege(current_user,'public.ff_partial_refund_operations','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS no_partial_table_access,
  has_function_privilege(current_user,'public.ff_claim_partial_refund(text,text,uuid,bigint,text,uuid)','EXECUTE') AS claim_available,
  has_function_privilege(current_user,'public.ff_finish_partial_refund(text,text,uuid,bigint,uuid,text,text)','EXECUTE') AS finish_available
  FROM pg_catalog.pg_roles r WHERE r.rolname = current_user`;

const CHECKS = Object.freeze([
  'original_role','restricted_role','primary_server','durable_storage','durable_commit',
  'read_only_probe','encrypted_connection','no_schema_create','no_executor_membership',
  'no_legacy_table_access','no_budget_table_access','no_partial_table_access',
  'claim_available','finish_available'
]);

async function inspectRefundLedgerConnection({env=process.env,PoolClass}={}) {
  let pool, client, discard=false;
  try {
    if(env.FF_REFUND_LEDGER_MODE!=='partial' || env.FF_REFUND_CURRENCY!=='USD') throw new Error();
    const config=ledgerPoolConfig(env);
    if(!config) throw new Error();
    const Pool=PoolClass || require('pg').Pool;
    pool=new Pool({...config,max:1});
    pool.on('error',()=>{}); // Never expose driver messages containing credentials.
    client=await pool.connect();
    if(client.getTransactionStatus()!=='I') throw new Error();
    await client.query('BEGIN READ ONLY');
    const result=await client.query(INSPECT);
    if(result.rows?.length!==1 || CHECKS.some(key=>result.rows[0][key]!==true)) throw new Error();
    await client.query('ROLLBACK');
    if(client.getTransactionStatus()!=='I') throw new Error();
    return Object.freeze({ok:true,checks:CHECKS,financialFunctionsCalled:0});
  } catch {
    discard=true;
    throw new Error('Refund ledger connection preflight failed');
  } finally {
    // Discard closes any failed/open read-only transaction; do not retry.
    try { if(client) client.release(discard); }
    catch { throw new Error('Refund ledger connection preflight failed'); }
    finally { if(pool) await pool.end().catch(()=>{}); }
  }
}
module.exports={inspectRefundLedgerConnection};
