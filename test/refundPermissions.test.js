const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const sql=fs.readFileSync(path.join(__dirname,'../migrations/003_partial_refund_permissions.sql'),'utf8');
test('staged privilege migration uses two non-login roles and no credentials',()=>{
  for(const role of ['executor','runtime']) assert.ok(sql.includes(`CREATE ROLE ff_refund_${role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`));
  assert.doesNotMatch(sql,/PASSWORD|IF NOT EXISTS/);
  assert.doesNotMatch(sql,/GRANT ff_refund_executor/);
});
test('runtime receives function execution but no direct table grant',()=>{
  assert.equal((sql.match(/GRANT EXECUTE ON FUNCTION/g)||[]).length,2);
  assert.doesNotMatch(sql,/GRANT (?:ALL|SELECT|INSERT|UPDATE|DELETE)[^;]*TO ff_refund_runtime/s);
  assert.match(sql,/FROM PUBLIC, ff_refund_runtime/);
  assert.equal((sql.match(/FROM PUBLIC;/g)||[]).length,3);
});
test('definer functions pin safe path and remove temporary schema creation',()=>{
  assert.equal((sql.match(/SET search_path = pg_catalog, pg_temp/g)||[]).length,2);
  assert.equal((sql.match(/SECURITY DEFINER;/g)||[]).length,2);
  assert.equal((sql.match(/OWNER TO ff_refund_executor;/g)||[]).length,2);
  assert.ok(sql.indexOf('REVOKE CREATE ON SCHEMA public FROM ff_refund_executor;')>sql.lastIndexOf('OWNER TO ff_refund_executor;'));
  assert.ok(sql.trim().endsWith('COMMIT;'));
});
