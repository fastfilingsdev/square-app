'use strict';

// Keep the existing filing-summary arithmetic in one pure helper. Both the
// report endpoint and writer must derive totals from their own single fetched
// classification result, never a second independently refreshed report.
function needsReview(item) {
  return item.classification_status !== 'taxable' && item.classification_status !== 'non_taxable';
}

function amount(value) {
  if (value == null) return 0; // Preserve existing absent optional-field behavior.
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error('Invalid classification report amount');
  }
  return value;
}

function filingTotals(orders) {
  const totals = { gross_sales_before_tax: 0, gross_sales_including_tax: 0,
    taxable_sales: 0, non_taxable_sales: 0, needs_review_sales: 0,
    tax_collected: 0, review_count: 0 };
  for (const order of orders) {
    totals.tax_collected += amount(order.tax_collected);
    for (const item of order.line_items || []) {
      const total = amount(item.total);
      const tax = amount(item.tax);
      totals.gross_sales_including_tax += total;
      totals.gross_sales_before_tax += total - tax;
      if (item.classification_status === 'taxable') totals.taxable_sales += total;
      else if (item.classification_status === 'non_taxable') totals.non_taxable_sales += total;
      else {
        totals.needs_review_sales += total;
        totals.review_count++;
      }
    }
  }
  if (Object.values(totals).some(value => !Number.isFinite(value))) {
    throw new Error('Invalid classification report totals');
  }
  return totals;
}

module.exports = { filingTotals, needsReview };
