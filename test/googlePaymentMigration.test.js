'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const migration=import('../tools/prepare-google-payment-migration.mjs');
test('replacement preserves adjacent functions and nested callbacks',async()=>{
  const {replaceFunction}=await migration;
  const source='// before\nfunction alpha() {\n  [1].map(function(x) { return x; });\n}\n\nfunction beta() {\n return 2;\n}\n';
  const result=replaceFunction(source,'alpha','function alpha() {\n return 1;\n}');
  assert.ok(result.startsWith('// before\nfunction alpha()'));assert.ok(result.endsWith('function beta() {\n return 2;\n}\n'));
  assert.throws(()=>replaceFunction(source+'\nfunction alpha() {\n}\n','alpha','x'));
  assert.throws(()=>replaceFunction(source,'missing','x'));
});
test('function replacement does not truncate column-zero nested braces or template strings',async()=>{
  const {replaceFunction}=await migration;
  const source='function alpha() {\n const template = `literal\n}\ntext`;\n if (true) {\n return template;\n}\n}\nfunction beta() {\n return 2;\n}\n';
  const result=replaceFunction(source,'alpha','function alpha() {\n return 1;\n}');
  assert.equal(result,'function alpha() {\n return 1;\n}\nfunction beta() {\n return 2;\n}\n');
  assert.throws(()=>replaceFunction(source,'alpha','function alpha() { invalid syntax }'));
});
