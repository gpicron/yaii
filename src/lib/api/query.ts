import {FieldName, IntegerValue, QueryOperator, TokenValue} from './base'

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

export interface TokenQuery extends ExactMatchLeafQuery<TokenValue | TokenValue[]> {
    readonly operator: QueryOperator.TOKEN
}

export interface TokenRangeQuery extends RangeMatchLeafQuery<TokenValue> {
    readonly operator: QueryOperator.TOKEN_RANGE
}

export interface NumberQuery extends ExactMatchLeafQuery<IntegerValue> {
    readonly operator: QueryOperator.NUMBER
}

export interface NumberRangeQuery extends RangeMatchLeafQuery<IntegerValue> {
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


export enum SortDirection {
    ASCENDING = 'ASC',
    DESCENDING = 'DESC'
}

export type SortClause =
    | {
    field: FieldName
    dir?: SortDirection
}
    | FieldName
