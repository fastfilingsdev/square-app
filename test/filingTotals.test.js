'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { filingTotals, needsReview } = require('../src/core/filingTotals');

test('single-snapshot totals retain existing classification arithmetic without input changes', () => {
  const orders = [{ tax_collected: 7, line_items: [
    { total: 107, tax: 7, classification_status: 'taxable' },
    { total: 20, classification_status: 'non_taxable' },
    { total: 5, classification_status: 'needs_review' },
    { total: 3, classification_status: 'new_unknown_value' }
  ] }];
  const before = structuredClone(orders);
  assert.deepEqual(filingTotals(orders), { gross_sales_before_tax: 128, gross_sales_including_tax: 135,
    taxable_sales: 107, non_taxable_sales: 20, needs_review_sales: 8, tax_collected: 7, review_count: 2 });
  assert.equal(orders.flatMap(x => x.line_items).filter(needsReview).length, 2);
  assert.deepEqual(orders, before);
});

test('empty classification snapshot yields all zero totals', () => {
  assert.ok(Object.values(filingTotals([])).every(value => value === 0));
});

test('missing classification is included in both review count and queue predicate', () => {
  const item = { total: 10 };
  assert.equal(needsReview(item), true);
  assert.equal(filingTotals([{ line_items: [item] }]).review_count, 1);
});

for (const field of ['total', 'tax', 'tax_collected']) {
  for (const value of ['12.50', '', true, NaN, Infinity, {}]) {
    test(`reject malformed ${field}: ${String(value)}`, () => {
      const order = { tax_collected: 0, line_items: [{ total: 10, tax: 0 }] };
      if (field === 'tax_collected') order[field] = value;
      else order.line_items[0][field] = value;
      assert.throws(() => filingTotals([order]), /Invalid classification report/);
    });
  }
}

test('reject finite inputs whose accumulated totals overflow', () => {
  assert.throws(() => filingTotals([{ line_items: [{ total: Number.MAX_VALUE }, { total: Number.MAX_VALUE }] }]),
    /Invalid classification report/);
});

test('retain zero, absent optional amounts and signed adjustments without coercion', () => {
  const result = filingTotals([{ line_items: [{ total: -10, tax: -1, classification_status: 'taxable' }, {}] }]);
  assert.equal(result.gross_sales_before_tax, -9);
  assert.equal(result.gross_sales_including_tax, -10);
  assert.equal(result.tax_collected, 0);
});
