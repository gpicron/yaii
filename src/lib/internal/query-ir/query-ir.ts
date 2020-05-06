import {
    AndQuery,
    FieldPresentQuery,
    LeafQuery,
    NotQuery,
    NumberQuery,
    NumberRangeQuery,
    OrQuery,
    Query,
    TokenQuery
} from '../../api/query'
import * as Long from 'long'
import {FieldName, QueryOperator} from "../../api/base"
import {ALL_EXP, Exp, NoneExp} from "./base"
import {Term, TermExp} from "./term-exp"
import {BooleanExpression} from "./boolean-exp"
import {INTERNAL_FIELDS} from "../utils"

enum TermPrefix {
    STRING = "0",
    NUMBER_L0 = "1",
    NUMBER_L1 = "2",
    NUMBER_L2 = "3",
    NUMBER_L3 = "4",
    NUMBER_L4 = "5",
    NUMBER_L5 = "6",
    NUMBER_L6 = "7",
    NUMBER_L7 = "8",
    NUMBER_L8 = "9",

    BOOLEAN_TRUE = "A",
    BOOLEAN_FALSE = "B"
}

export const TERM_TRUE = TermPrefix.BOOLEAN_TRUE
export const TERM_FALSE = TermPrefix.BOOLEAN_FALSE

export function stringToTerm(token: string): Term {
    return TermPrefix.STRING + token
}
/*
function stringToTermBuffer(token: string): Buffer {
    const data = Buffer.from(token, 'utf8')
    const result = Buffer.allocUnsafe(data.length + 1)

    result[0] = TermPrefix.BUFFER_OR_STRING
    data.copy(result, 1)

    return result
}
*/

const ENCODING_DIGITS = "+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

function extractNumberTerms(long: Long): Term[] {
    const hi = long.high
    const lo = long.low

    // we keep only 54 bits 9 * 6 bits digits (in little endian order)

    const digits = [
        lo & 0x3F,

        lo >>> 6 & 0x3F,
        lo >>> 12 & 0x3F,
        lo >>> 18 & 0x3F,
        lo >>> 24 & 0x3F,
        (lo >>> 30 & 0x3F) + (hi << 2 & 0x3F),
        hi >>> 4 & 0x3F,
        hi >>> 10 & 0x3F,
        hi >>> 16 & 0x3F
    ]

    const result = [
        TermPrefix.NUMBER_L0 + ENCODING_DIGITS[digits[0]]
    ]

    let next = ""
    for (let i = 8; i > 0; i--) {
        next += ENCODING_DIGITS[digits[i]]
        result.push(ENCODING_DIGITS[i+3] + next)
    }

    return result
}

const NUMBER_SHIFT = Long.fromNumber(Number.MAX_SAFE_INTEGER).add(1)

export function numberToTerms(v: number): Term[] {
    const long = Long.fromNumber(Math.floor(v)).add(NUMBER_SHIFT)
    return extractNumberTerms(long)
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

            let values = q.value

            if (!Array.isArray(values)) values = [values]

            const exps = values.map(value => {
                if (typeof value === 'string') {
                    return new TermExp(fieldNameOrAll(q), stringToTerm(value))
                } else if (typeof value === 'boolean') {
                    return new TermExp(fieldNameOrAll(q), value ? TERM_TRUE : TERM_FALSE)
                }
            }).filter(it => typeof it !== 'undefined') as Exp[]

            return new BooleanExpression(
                exps
            )

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
            const termExps = [new TermExp(fieldName, terms[0]), new TermExp(fieldName, terms[8])]

            return new BooleanExpression(undefined, termExps)
        }

        case QueryOperator.NUMBER_RANGE: {
            const q = query as NumberRangeQuery

            let minInclusive: Long
            if (q.min == Number.NEGATIVE_INFINITY) {
                minInclusive = Long.fromNumber(Number.MIN_SAFE_INTEGER).add(NUMBER_SHIFT)
            } else {
                minInclusive = q.minInclusive ? Long.fromNumber(q.min) : Long.fromNumber(q.min).add(1)
                minInclusive = minInclusive.add(NUMBER_SHIFT)
            }

            let maxExclusive: Long
            if (q.max == Number.POSITIVE_INFINITY) {
                maxExclusive = Long.fromNumber(Number.MAX_SAFE_INTEGER).add(NUMBER_SHIFT).add(1)
            } else {
                maxExclusive = q.maxInclusive ? Long.fromNumber(q.max).add(1) : Long.fromNumber(q.max)
                maxExclusive = maxExclusive.add(NUMBER_SHIFT)
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

export function rangeToExp(field: FieldName, fromRange: Long, level: number, fromRem: number, toRem: number): Exp {
    if (level == 0) {
        const rangeTerm = extractNumberTerms(fromRange.shiftLeft(6))[8]

        const remTerms = new Array<Exp>()

        for (let i = fromRem; i < toRem; i++) {
            const p = TermPrefix.NUMBER_L0 + ENCODING_DIGITS[i]
            remTerms.push(new TermExp(field, p))
        }

        return new BooleanExpression(remTerms, [new TermExp(field, rangeTerm)])
    } else {
        let rangeTerm = extractNumberTerms(fromRange.shiftLeft((level+1) * 6))[9-level]
        rangeTerm = rangeTerm.substring(0, rangeTerm.length-1)

        const terms = new Array<Exp>()
        for (let range = fromRem; range < toRem; range++) {
            terms.push(new TermExp(field, rangeTerm + ENCODING_DIGITS[range]))
        }

        return new BooleanExpression(terms)
    }
}

export function addRange(field: FieldName, from: Long, to: Long, level: number): Exp[] {
    let terms = new Array<Exp>()
    if (from.gte(to)) return terms

    const precision = Long.fromNumber(64)
    const fromRange = from.div(precision)
    const fromRem = from.mod(precision).toNumber()
    const toRange = to.div(precision)
    const toRem = to.mod(precision).toNumber()

    if (fromRem == 0 && fromRange.equals(toRange)) {
        terms = terms.concat(addRange(field, fromRange, fromRange.add(1), level + 1))
    } else {
        if (fromRange.equals(toRange)) {
            terms.push(rangeToExp(field, fromRange, level, fromRem, toRem))
        } else {
            terms.push(rangeToExp(field, fromRange, level, fromRem, 64))
        }
    }

    terms = terms.concat(addRange(field, fromRange.add(1), toRange, level + 1))

    if (toRem != 0 && !fromRange.equals(toRange)) {
        terms.push(rangeToExp(field, toRange, level, 0, toRem))
    }

    return terms
}
