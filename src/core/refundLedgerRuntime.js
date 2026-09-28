const { createRefundLedger } = require('./refundLedger');
const { createPartialRefundLedger } = require('./partialRefundLedger');

function ledgerPoolConfig(env) {
  // Deliberately no generic DATABASE_URL fallback: never attach to another app's DB.
  const raw = env.FF_REFUND_LEDGER_DATABASE_URL;
  const scope = env.FF_REFUND_PROVIDER_SCOPE;
  if (!raw && !scope) return null;
  try {
    if (typeof raw !== 'string' || typeof scope !== 'string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(scope)) throw new Error();
    const url = new URL(raw);
    if (!['postgres:', 'postgresql:'].includes(url.protocol) || !url.hostname || !url.username || !url.password ||
        !/^\/[a-zA-Z0-9_-]+$/.test(url.pathname) || url.search || url.hash) throw new Error();
    const tlsMode = env.FF_REFUND_LEDGER_TLS_MODE || 'verify-full';
    // The internal Render endpoint uses a self-signed certificate. Its explicit
    // mode is allowed only for a Render-internal host, never an external URL.
    const privateHost = /^dpg-[a-z0-9]+-[a-z]$/.test(url.hostname);
    if (tlsMode !== 'verify-full' && !(tlsMode === 'render-internal' && privateHost)) throw new Error();
    const port = url.port ? Number(url.port) : 5432;
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error();
    return {
      host: url.hostname, port,
      user: decodeURIComponent(url.username), password: decodeURIComponent(url.password), database:url.pathname.slice(1),
      ssl:{rejectUnauthorized:tlsMode === 'verify-full',minVersion:'TLSv1.2'},
      max:4, connectionTimeoutMillis:5000, idleTimeoutMillis:10000,
      statement_timeout:5000, query_timeout:7000, lock_timeout:3000,
      idle_in_transaction_session_timeout:5000,
      application_name:'fastfilings-refund-ledger',
      options:'-c synchronous_commit=on -c search_path=public',
      pipeline:false
    };
  } catch {
    throw new Error('Invalid refund ledger configuration'); // Never include URL/credentials.
  }
}

const CHECK_SESSION = `SELECT pg_is_in_recovery() AS recovery,
  current_setting('synchronous_commit') AS synchronous_commit,
  current_setting('transaction_read_only') AS read_only,
  current_setting('fsync') AS fsync,
  current_user = session_user AS original_role,
  NOT (r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls) AS restricted_role
  FROM pg_catalog.pg_roles r WHERE r.rolname = current_user`;

function createRefundLedgerRuntime({env=process.env, PoolClass, onPoolError=()=>{}}={}) {
  const config = ledgerPoolConfig(env);
  if (!config) return Object.freeze({routerOptions:{},close:async()=>{}});
  const mode = env.FF_REFUND_LEDGER_MODE || 'single';
  if (!['single','partial'].includes(mode) || (mode === 'partial' && env.FF_REFUND_CURRENCY !== 'USD')) {
    throw new Error('Invalid refund ledger mode or currency');
  }
  const Pool = PoolClass || require('pg').Pool;
  const pool = new Pool(config);
  pool.on('error',()=>{try{onPoolError('Refund ledger connection unavailable');}catch{/* no secret-bearing idle errors */}});
  let closed=false;
  async function query(sql,values) {
    if(closed) throw new Error('Refund ledger unavailable');
    let client;
    let discard=false;
    try {
      client=await pool.connect();
      if(client.getTransactionStatus() !== 'I') throw new Error('Unsafe ledger session');
      const result=await client.query(CHECK_SESSION);
      const s=result.rows?.[0];
      if(result.rows?.length!==1 || s.recovery!==false || s.synchronous_commit!=='on' || s.read_only!=='off' || s.fsync!=='on' ||
          s.original_role!==true || s.restricted_role!==true ||
          client.getTransactionStatus()!=='I') throw new Error('Unsafe ledger session');
      const written=await client.query(sql,values);
      // Claims must be committed before returning permission to invoke provider.
      if(client.getTransactionStatus()!=='I') throw new Error('Uncommitted ledger write');
      return written;
    } catch {
      discard=true;
      throw new Error('Refund ledger operation unconfirmed');
    } finally {
      if(client) client.release(discard);
    }
  }
  const refundLedger=(mode === 'partial' ? createPartialRefundLedger : createRefundLedger)({query});
  return Object.freeze({
    routerOptions:Object.freeze({refundLedger,providerScope:env.FF_REFUND_PROVIDER_SCOPE,
      ...(mode === 'partial' ? {refundCurrency:env.FF_REFUND_CURRENCY} : {})}),
    async close(){if(!closed){closed=true;await pool.end();}}
  });
}
module.exports={createRefundLedgerRuntime,ledgerPoolConfig};
