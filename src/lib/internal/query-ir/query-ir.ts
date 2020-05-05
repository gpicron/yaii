import {
    AndQuery,
    FieldPresentQuery,
    LeafQuery,
    NotQuery,
    NumberQuery,
    NumberRangeQuery,
    OrQuery,
    Query,
    QueryOperator,
    TokenQuery
} from '../../api/query'
import * as Long from 'long'
import {FieldName} from "../../api/base"
import {ALL_EXP, Exp, NoneExp} from "./base"
import {Term, TermExp} from "./term-exp"
import {BooleanExpression} from "./boolean-exp"
import {INTERNAL_FIELDS} from "../utils"

enum TermPrefix {
    BUFFER_OR_STRING = 0,
    NUMBER_L0 = 1,
    NUMBER_L1 = 2,
    NUMBER_L2 = 3,
    NUMBER_L3 = 4,
    NUMBER_L4 = 5,
    NUMBER_L5 = 6,
    NUMBER_L6 = 7,
    BOOLEAN_TRUE = 8,
    BOOLEAN_FALSE = 9
}

export const TERM_TRUE = Buffer.of(TermPrefix.BOOLEAN_TRUE)
export const TERM_FALSE = Buffer.of(TermPrefix.BOOLEAN_TRUE)

export function stringToTerm(token: string): Term {
    const data = Buffer.from(token, 'utf8')
    const result = Buffer.allocUnsafe(data.length + 1)

    result[0] = TermPrefix.BUFFER_OR_STRING
    data.copy(result, 1)

    return result
}

export function numberToTerms(v: number): Term[] {
    const bint = Long.fromNumber(Math.floor(v)).add(Number.MAX_SAFE_INTEGER)

    const bytes = bint.toBytesBE()

    const result = []

    const last = bytes.pop() as number

    for (let i = 0; i < 6; i++) {
        bytes[0] = TermPrefix.NUMBER_L1 + i
        result.push(Buffer.from(bytes))
        bytes.pop()
    }

    result.push(Buffer.of(TermPrefix.NUMBER_L0, last))

    return result
}

function fieldNameOrAll(q: LeafQuery): FieldName | INTERNAL_FIELDS.ALL {
    return q.field || INTERNAL_FIELDS.ALL
}

export function buildExpression(query: Query): Exp {
    switch (query.operator) {
        case QueryOperator.ALL:
            return ALL_EXP
        case QueryOperator.TOKEN: {
            const q = query as TokenQuery

            const value = q.value

            if (typeof value === 'string') {
                return new TermExp(fieldNameOrAll(q), stringToTerm(value))
            } else if (typeof value === 'boolean') {
                return new TermExp(fieldNameOrAll(q), value ? TERM_TRUE : TERM_FALSE)
            }
        }
        case QueryOperator.AND: {
            const q = query as AndQuery
            const operands = q.operands.map(buildExpression)

            return new BooleanExpression(undefined, operands)
        }
        case QueryOperator.OR: {
            const q = query as OrQuery

            const operands = q.operands.map(buildExpression)

            return new BooleanExpression(operands)
        }
        case QueryOperator.NOT: {
            const q = query as NotQuery

            return new BooleanExpression(undefined, undefined, [buildExpression(q.operand)])
        }
        case QueryOperator.NUMBER: {
            const q = query as NumberQuery
            const fieldName = fieldNameOrAll(q)
            const terms = numberToTerms(q.value)
            const termExps = [new TermExp(fieldName, terms[5]), new TermExp(fieldName, terms[6])]

            return new BooleanExpression(undefined, termExps)
        }

        case QueryOperator.NUMBER_RANGE: {
            const q = query as NumberRangeQuery

            const ZERO = BigInt(0)
            const ONE = BigInt(1)

            const BI_MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)

            let minInclusive: bigint
            if (q.min == Number.NEGATIVE_INFINITY) {
                minInclusive = ZERO
            } else {
                minInclusive = q.minInclusive ? BigInt(q.min) : BigInt(q.min) + ONE
                minInclusive += BI_MAX_SAFE
            }

            let maxExclusive: bigint
            if (q.max == Number.POSITIVE_INFINITY) {
                maxExclusive = BI_MAX_SAFE + BI_MAX_SAFE + ONE
            } else {
                maxExclusive = q.maxInclusive ? BigInt(q.max) + ONE : BigInt(q.max)
                maxExclusive += BI_MAX_SAFE
            }

            const allterms = addRange(q.field, minInclusive, maxExclusive, 0)

            return allterms.length == 0 ? new NoneExp() : new BooleanExpression(allterms)
        }
        case QueryOperator.HAS_FIELD: {
            const q = query as FieldPresentQuery
            return new TermExp(INTERNAL_FIELDS.FIELDS, stringToTerm(q.field))
        }

        case QueryOperator.TOKEN_RANGE:
        case QueryOperator.TEXT_CONTAINS:
            throw new Error('Not yet implemented.')
    }
}

export function rangeToExp(field: FieldName, fromRange: bigint, level: number, fromRem: number, toRem: number): Exp {
    if (level == 0) {
        let rangeTerm = Buffer.allocUnsafe(8)
        rangeTerm.writeBigUInt64BE(fromRange, 0)
        rangeTerm.writeUInt8(TermPrefix.NUMBER_L1, 1)
        rangeTerm = rangeTerm.slice(1, 8)

        const remTerms = new Array<Exp>()

        for (let i = fromRem; i < toRem; i++) {
            const p = Buffer.of(TermPrefix.NUMBER_L0, i)
            remTerms.push(new TermExp(field, p))
        }

        return new BooleanExpression(remTerms, [new TermExp(field, rangeTerm)])
    } else {
        let base = Buffer.allocUnsafe(9)
        base.writeBigUInt64BE(fromRange, 0)
        base.writeUInt8(level + 1, level + 1)
        base = base.slice(level + 1, 9)

        const terms = new Array<Exp>()

        for (let range = fromRem; range < toRem; range++) {
            const rt = Buffer.allocUnsafe(base.length)
            base.copy(rt)
            rt.writeUInt8(range, base.length - 1)

            terms.push(new TermExp(field, rt))
        }

        return new BooleanExpression(terms)
    }
}

const ONE = BigInt(1)

export function addRange(field: FieldName, from: bigint, to: bigint, level: number): Exp[] {
    let terms = new Array<Exp>()
    if (from >= to) return terms

    const precision = BigInt(256)
    const fromRange = from / precision
    const fromRem = Number(from % precision)
    const toRange = to / precision
    const toRem = Number(to % precision)

    if (fromRem == 0 && fromRange != toRange) {
        terms = terms.concat(addRange(field, fromRange, fromRange + ONE, level + 1))
    } else {
        if (fromRange == toRange) {
            terms.push(rangeToExp(field, fromRange, level, fromRem, toRem))
        } else {
            terms.push(rangeToExp(field, fromRange, level, fromRem, 256))
        }
    }

    terms = terms.concat(addRange(field, fromRange + ONE, toRange, level + 1))

    if (toRem != 0 && fromRange != toRange) {
        terms.push(rangeToExp(field, toRange, level, 0, toRem))
    }

    return terms
}
