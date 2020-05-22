import {
    AllQuery,
    AndQuery,
    FieldPresentQuery,
    NotQuery,
    NumberQuery,
    NumberRangeQuery,
    OrQuery,
    Query,
    TokenQuery
} from "./query"
import {FieldName, IntegerValue, QueryOperator, TokenValue} from "./base"

export function token(token: TokenValue | TokenValue[], field?: FieldName): TokenQuery {
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

export function number(value: IntegerValue, field?: FieldName): NumberQuery {
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
    min: IntegerValue | undefined,
    max: IntegerValue | undefined,
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

export function and(...queries: Query[]): AndQuery {
    return {
        operator: QueryOperator.AND,
        operands: queries
    }
}

export function or(...queries: Query[]): OrQuery {
    return {
        operator: QueryOperator.OR,
        operands: queries
    }
}

export function not(query: Query): NotQuery {
    return {
        operator: QueryOperator.NOT,
        operand: query
    }
}
