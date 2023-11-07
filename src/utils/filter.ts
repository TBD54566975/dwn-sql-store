import { Filter } from '@tbd54566975/dwn-sdk-js';
import { DynamicModule, ExpressionBuilder, OperandExpression, SelectQueryBuilder, SqlBool } from 'kysely';
import { sanitizedValue } from './sanitize.js';

/**
 * Takes multiple Filters and returns a single query.
 * Each filter is evaluated as an OR operation.
 *
 * @param filters Array of filters to be evaluated as OR operations
 * @param query the incoming QueryBuilder.
 * @returns The modified QueryBuilder respecting the provided filters.
 */
export function filterSelectQuery<DB = unknown, TB extends keyof DB = keyof DB, O = unknown>(
  filters: Filter[],
  query: SelectQueryBuilder<DB, TB, O>
): SelectQueryBuilder<DB, TB, O> {
  return query.where((eb) => {
    // we are building multiple OR queries out of each individual filter.
    const or: OperandExpression<SqlBool>[] = [];
    for (let filter of filters) {
      // processFilter will take a single filter adding it to the query to be evaluated as an OR operation with the other filters.
      or.push(processFilter(eb, filter));
    }
    // Evaluate the array of expressions as an OR operation.
    return eb.or(or);
  });
}

/**
 * Returns an array of OperandExpressions for a single filter.
 * Each property within the filter is evaluated as an AND operand,
 * if a property has an array of values it will treat it as a OneOf (IN) within the overall AND query.
 * This way each Filer has to be a complete match, but the collection of filters can be evaluated as chosen.
 *
 * @param eb The ExpressionBuilder from the query.
 * @param filter The filter to be evaluated.
 * @returns An array of OperandExpressions to be evaluated by the caller.
 */
function processFilter<DB = unknown, TB extends keyof DB = keyof DB>(
  eb: ExpressionBuilder<DB, TB>,
  filter: Filter
):OperandExpression<SqlBool> {
  const andOperands: OperandExpression<SqlBool>[] = [];
  for (let property in filter) {
    const value = filter[property];
    const column = new DynamicModule().ref(property);
    if (Array.isArray(value)) { // OneOfFilter
      andOperands.push(eb(column, 'in', value));
    } else if (typeof value === 'object') { // RangeFilter
      if (value.gt) {
        andOperands.push(eb(column, '>', sanitizedValue(value.gt)));
      }
      if (value.gte) {
        andOperands.push(eb(column, '>=', sanitizedValue(value.gte)));
      }
      if (value.lt) {
        andOperands.push(eb(column, '<', sanitizedValue(value.lt)));
      }
      if (value.lte) {
        andOperands.push(eb(column, '<=', sanitizedValue(value.lte)));
      }
    } else { // EqualFilter
      andOperands.push(eb(column, '=', sanitizedValue(value)));
    }
  }

  // evaluate the the collected operands as an AND operation.
  return eb.and(andOperands);
}