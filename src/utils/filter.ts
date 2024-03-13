import { Filter } from '@tbd54566975/dwn-sdk-js';
import { DynamicModule, ExpressionBuilder, OperandExpression, SelectQueryBuilder, SqlBool } from 'kysely';
import { extractTagsAndSanitizeFilters, sanitizedValue } from './sanitize.js';
import { DwnDatabaseType } from '../types.js';

/**
 * Takes multiple Filters and returns a single query.
 * Each filter is evaluated as an OR operation.
 *
 * @param filters Array of filters to be evaluated as OR operations
 * @param query the incoming QueryBuilder.
 * @returns The modified QueryBuilder respecting the provided filters.
 */
export function filterSelectQuery<DB = DwnDatabaseType, TB extends keyof DB = keyof DB, O = unknown>(
  filters: Filter[],
  query: SelectQueryBuilder<DB, TB, O>
): SelectQueryBuilder<DB, TB, O> {
  const extractedFilters = extractTagsAndSanitizeFilters(filters);

  return query.where((eb) =>
    eb.or(extractedFilters.map(({ filter, tag }) =>
      processFilterWithTag(eb, filter, tag)
    ))
  );
}

/**
 * Returns an array of OperandExpressions for a single filter.
 * Each property within the filter is evaluated as an AND operand,
 * if a property has an array of values it will treat it as a OneOf (IN) within the overall AND query.
 * This way each Filer has to be a complete match, but the collection of filters can be evaluated as chosen.
 *
 * @param eb The ExpressionBuilder from the query.
 * @param filter The filter to be evaluated.
 * @returns An array of OperandExpression<SqlBool> to be evaluated by the caller.
 */
function processFilter<DB = DwnDatabaseType, TB extends keyof DB = keyof DB>(
  eb: ExpressionBuilder<DB, TB>,
  filter: Filter
):OperandExpression<SqlBool>[] {
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

  return andOperands;
}


/**
 * Returns an array of OperandExpressions for a single filter.
 * Each property within the filter is evaluated as an AND operand,
 * if a property has an array of values it will treat it as a OneOf (IN) within the overall AND query.
 * This way each Filer has to be a complete match, but the collection of filters can be evaluated as chosen.
 *
 * @param eb The ExpressionBuilder from the query.
 * @param filter The filter to be evaluated.
 * @returns An a single OperandExpression that represents an AND operation for all of the individual filters to be used by the caller.
 */
function processFilterWithTag<DB = DwnDatabaseType, TB extends keyof DB = keyof DB>(
  eb: ExpressionBuilder<DB, TB>,
  filter: Filter,
  tag: Filter,
):OperandExpression<SqlBool> {

  const tagColumn = new DynamicModule().ref('tag');
  const valueNumber = new DynamicModule().ref('valueNumber');
  const valueString = new DynamicModule().ref('valueString');

  // process the regular filters
  const andOperands = processFilter(eb, filter);

  // process each tag and add it to the andOperands from the rest of the filters
  for (let property in tag) {
    andOperands.push(eb(tagColumn, '=', property));
    const value = tag[property];
    if (Array.isArray(value)) { // OneOfFilter
      if (value.some(val => typeof val === 'number')) {
        andOperands.push(eb(valueNumber, 'in', value));
      } else {
        andOperands.push(eb(valueString, 'in', value.map(v => String(v))));
      }
    } else if (typeof value === 'object') { // RangeFilter
      if (value.gt) {
        if (typeof value.gt === 'number') {
          andOperands.push(eb(valueNumber, '>', value.gt));
        } else {
          andOperands.push(eb(valueString, '>', String(value.gt)));
        }
      }
      if (value.gte) {
        if (typeof value.gte === 'number') {
          andOperands.push(eb(valueNumber, '>=', value.gte));
        } else {
          andOperands.push(eb(valueString, '>=', String(value.gte)));
        }
      }
      if (value.lt) {
        if (typeof value.lt === 'number') {
          andOperands.push(eb(valueNumber, '<', value.lt));
        } else {
          andOperands.push(eb(valueString, '<', String(value.lt)));
        }
      }
      if (value.lte) {
        if (typeof value.lte === 'number') {
          andOperands.push(eb(valueNumber, '<=', value.lte));
        } else {
          andOperands.push(eb(valueString, '<=', String(value.lte)));
        }
      }
    } else { // EqualFilter
      if (typeof value === 'number') {
        andOperands.push(eb(valueNumber, '=', value));
      } else {
        andOperands.push(eb(valueString, '=', String(value)));
      }
    }
  }

  // evaluate the the collected operands as an AND operation.
  return eb.and(andOperands);
}