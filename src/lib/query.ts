import { FieldName, NumericFieldValue, TokenValue } from '../yaii-types'

export enum QueryOperator {
    TOKEN = 'TOKEN',
    TOKEN_RANGE = 'TOKEN_RANGE',
    NUMBER = 'NUMBER',
    NUMBER_RANGE = 'NUMBER_RANGE',
    TEXT_CONTAINS = 'TEXT_CONTAINS',
    ALL = 'ALL',
    OR = 'OR',
    AND = 'AND',
    NOT = 'NOT',
    HAS_FIELD = 'HAS_FIELD'
}

export interface Query {
    readonly operator: QueryOperator
}

export interface LeafQuery extends Query {
    readonly field?: FieldName
}

export interface ExactMatchLeafQuery<T> extends LeafQuery {
    readonly value: T
}

interface RangeMatchLeafQuery<T> extends LeafQuery {
    readonly field: FieldName
    readonly min: T
    readonly max: T
    readonly minInclusive: boolean
    readonly maxInclusive: boolean
}

export interface TokenQuery extends ExactMatchLeafQuery<TokenValue> {
    readonly operator: QueryOperator.TOKEN
}

export interface TokenRangeQuery extends RangeMatchLeafQuery<TokenValue> {
    readonly operator: QueryOperator.TOKEN_RANGE
}

export interface NumberQuery extends ExactMatchLeafQuery<NumericFieldValue> {
    readonly operator: QueryOperator.NUMBER
}

export interface NumberRangeQuery
    extends RangeMatchLeafQuery<NumericFieldValue> {
    readonly operator: QueryOperator.NUMBER_RANGE
}

export interface FieldPresentQuery extends LeafQuery {
    readonly field: FieldName
    readonly operator: QueryOperator.HAS_FIELD
}

export interface AllQuery extends Query {
    readonly operator: QueryOperator.ALL
}

export interface OrQuery extends Query {
    readonly operator: QueryOperator.OR
    readonly operands: Query[]
}

export interface AndQuery extends Query {
    readonly operator: QueryOperator.AND
    readonly operands: Query[]
}

export interface NotQuery extends Query {
    readonly operator: QueryOperator.NOT
    readonly operand: Query
}

export function token(token: TokenValue, field?: FieldName): TokenQuery {
    return {
        operator: QueryOperator.TOKEN,
        field: field,
        value: token
    }
}

export function all(): AllQuery {
    return {
        operator: QueryOperator.ALL
    }
}

export function number(
    value: NumericFieldValue,
    field?: FieldName
): NumberQuery {
    return {
        operator: QueryOperator.NUMBER,
        field: field,
        value: value
    }
}

export function present(field: FieldName): FieldPresentQuery {
    return {
        operator: QueryOperator.HAS_FIELD,
        field: field
    }
}

export function numberRange(
    field: FieldName,
    min: NumericFieldValue | undefined,
    max: NumericFieldValue | undefined,
    minInclusive: boolean = true,
    maxInclusive: boolean = false
): NumberRangeQuery {
    if (!min && !max) throw new Error('invalid range')

    min = min || Number.NEGATIVE_INFINITY
    max = max || Number.POSITIVE_INFINITY

    return {
        operator: QueryOperator.NUMBER_RANGE,
        field: field,
        min: min,
        max: max,
        minInclusive: minInclusive,
        maxInclusive: maxInclusive
    }
}

export function and(...Querys: Query[]): AndQuery {
    return {
        operator: QueryOperator.AND,
        operands: Querys
    }
}

export function or(...Querys: Query[]): OrQuery {
    return {
        operator: QueryOperator.OR,
        operands: Querys
    }
}
