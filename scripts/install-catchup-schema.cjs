'use strict';
const {createHash}=require('node:crypto');
const hashes=['5ce7b518483da9290ace810a2c6190e4d814d50d0e99e6e0d199f88f735d161d','0a34b9ef8738b2537e1807346fe885c9d5fdef163533132351abd8ac1adb5de7','b2b99c7dccca870c580fb407c8fbad93ccd39afff5581014d2d99b3cd37a6246'];
const tables=['ff_catchup_operations','ff_catchup_periods','ff_catchup_confirmations','ff_catchup_quotes'];
const names=['ff_claim_catchup','ff_finish_catchup','ff_claim_catchup_confirmation','ff_finish_catchup_confirmation','ff_save_catchup_quote','ff_read_catchup_quote','ff_accept_catchup_quote','ff_read_accepted_catchup_quote'];
const literals=xs=>xs.map(x=>"'"+x+"'").join(',');
const good=r=>r.rows.length===1&&Object.values(r.rows[0]).length>0&&Object.values(r.rows[0]).every(x=>x===true);
const verify=`SELECT
 (SELECT count(*)=4 AND bool_and(pg_get_userbyid(c.relowner)='fastfilings_operations_ledger_user' AND NOT has_table_privilege('ff_refund_app',c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN (${literals(tables)})) AS table_acl,
 (SELECT count(*)=8 AND bool_and(p.prosecdef AND pg_get_userbyid(p.proowner)='ff_subscription_executor' AND p.proconfig @> ARRAY['search_path=pg_catalog, pg_temp'] AND has_function_privilege('ff_refund_app',p.oid,'EXECUTE')) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname IN (${literals(names)})) AS function_acl,
 NOT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE n.nspname='public' AND p.proname IN (${literals(names)}) AND a.grantee=0 AND a.privilege_type='EXECUTE') AS no_public_execute,
 NOT has_schema_privilege('ff_refund_app','public','CREATE') AND NOT pg_has_role('ff_refund_app','ff_subscription_executor','MEMBER') AS serving_restricted,
 NOT has_schema_privilege('ff_subscription_executor','public','CREATE') AND NOT pg_has_role(current_user,'ff_subscription_executor','USAGE') AND NOT pg_has_role(current_user,'ff_subscription_executor','SET') AS temporary_grants_removed,
 (SELECT NOT(rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls) FROM pg_roles WHERE rolname='ff_subscription_executor') AS executor_restricted,
 ${tables.map((t,i)=>`NOT EXISTS(SELECT 1 FROM public.${t}) AS empty_${i}`).join(',')}`;
async function install({client,migrations,approved,assertDisabled}){
 let stage='preflight',commitAttempted=false;
 try{
  if(approved!==true||typeof assertDisabled!=='function'||!Array.isArray(migrations)||migrations.length!==3||
    migrations.some((s,i)=>createHash('sha256').update(s).digest('hex')!==hashes[i]))throw Error();
  assertDisabled();await client.query('BEGIN');
  await client.query("SET LOCAL synchronous_commit=on; SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='10s'");
  const before=await client.query(`SELECT current_database()='fastfilings_operations_ledger' AS database_ok,
    current_user='fastfilings_operations_ledger_user' AS owner_ok,
    NOT pg_is_in_recovery() AND current_setting('fsync')='on' AND current_setting('synchronous_commit')='on' AS durable,
    NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN (${literals(tables)})) AS tables_absent,
    NOT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname IN (${literals(names)})) AS functions_absent,
    to_regclass('public.ff_membership_claims') IS NOT NULL AND to_regclass('public.ff_cancellation_claims') IS NOT NULL AS prerequisites`);
  if(!good(before))throw Error();
  stage='install';
  for(const sql of migrations){
   if((sql.match(/^BEGIN;$/gm)||[]).length!==1||(sql.match(/^COMMIT;$/gm)||[]).length!==1)throw Error();
   await client.query(sql.replace(/^BEGIN;\r?\n/m,'').replace(/^COMMIT;\s*$/m,''));
  }
  stage='verify';if(!good(await client.query(verify)))throw Error();assertDisabled();
  stage='commit';commitAttempted=true;await client.query('COMMIT');
  stage='readback';if(!good(await client.query(verify)))throw Error();
  return {ok:true,installedFunctions:8,installedTables:4,emptyTables:true,functionOnlyAccess:true,automationActivated:false,realPayments:0,emails:0};
 }catch{try{await client.query('ROLLBACK');}catch{}return {ok:false,stage,commitAttempted,automaticRetryAllowed:false,realPayments:0,emails:0};}
}
module.exports={install,hashes,verify};
