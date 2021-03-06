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
import {FieldName, QueryOperator, TokenValue} from "../../api/base"
import {ALL_EXP, Exp, NoneExp} from "./base"
import {Term, TermExp} from "./term-exp"
import {BooleanExpression} from "./boolean-exp"
import {INTERNAL_FIELDS} from "../utils"
import {BaseSegment} from "../segments/segment"
import {CachedFilter} from "./cached-filter"

export enum TermPrefix {
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

export function termToToken(term: Term): TokenValue {
    switch (term[0]) {
        case TermPrefix.STRING:
            return term.substring(1)
        case TermPrefix.BOOLEAN_TRUE:
            return true
        case TermPrefix.BOOLEAN_FALSE:
            return false
        default:
            throw new Error("Not yet implemented")
    }
}

export function stringToTerm(token: string): Term {
    return TermPrefix.STRING + token
}

const ENCODING_DIGITS = "+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

export function convertU32ToLex64(n: number): string {
    const long = Long.fromNumber(n, true)
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
    ]

    let next = ""
    for (let i = 0; i < 6; i++) {
        next += ENCODING_DIGITS[digits[i]]
    }

    return next
}

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

export function buildFilterExpression(query: Query, segment: BaseSegment): Exp {
    switch (query.operator) {
        case QueryOperator.ALL:
            return ALL_EXP
        case QueryOperator.TOKEN: {
            const q = query as TokenQuery

            if (q.field === INTERNAL_FIELDS.FILTER_CACHE) {
                return q as CachedFilter
            }

            let values = q.value

            if (!Array.isArray(values)) values = [values]

            const exps = values.map(value => {
                if (typeof value === 'string') {
                    return new TermExp(fieldNameOrAll(q), stringToTerm(value))
                } else if (typeof value === 'boolean') {
                    return new TermExp(fieldNameOrAll(q), value ? TERM_TRUE : TERM_FALSE)
                }
            }).filter(it => typeof it !== 'undefined' && segment.mayMatch(it)) as Exp[]

            if (exps.length == 0) {
                return  new NoneExp()
            } else if (exps.length == 1) {
                return exps[0]
            } else {
                return new BooleanExpression(exps)
            }

        }
        case QueryOperator.AND: {
            const q = query as AndQuery
            const operands = q.operands.map(it => buildFilterExpression(it, segment))

            return new BooleanExpression(undefined, operands)
        }
        case QueryOperator.OR: {
            const q = query as OrQuery

            const operands = q.operands.map(it => buildFilterExpression(it, segment))

            return new BooleanExpression(operands)
        }
        case QueryOperator.NOT: {
            const q = query as NotQuery

            return new BooleanExpression(undefined, undefined, [buildFilterExpression(q.operand, segment)])
        }
        case QueryOperator.NUMBER: {
            const q = query as NumberQuery
            const fieldName = fieldNameOrAll(q)
            const terms = numberToTerms(q.value)
            const termExps = [new TermExp(fieldName, terms[0]), new TermExp(fieldName, terms[8])]

            if (segment.mayMatch(termExps[0]) && segment.mayMatch(termExps[1])) {
                return new BooleanExpression(undefined, termExps)
            } else {
                return new NoneExp()
            }

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

            const allterms = addRange(q.field, minInclusive, maxExclusive, 0, segment)


            return allterms.length == 0 ? new NoneExp() : new BooleanExpression(allterms)
        }
        case QueryOperator.HAS_FIELD: {
            const q = query as FieldPresentQuery
            return new TermExp(INTERNAL_FIELDS.FIELDS, stringToTerm(q.field))
        }

        case QueryOperator.TOKEN_RANGE:
        case QueryOperator.TEXT_CONTAINS:
        default:
            throw new Error('Not yet implemented.')
    }
}

export function rangeToExp(field: FieldName, fromRange: Long, level: number, fromRem: number, toRem: number, segment: BaseSegment): Exp {
    if (level == 0) {
        const rangeTerm = extractNumberTerms(fromRange.shiftLeft(6))[8]
        const rangeTermExp = new TermExp(field, rangeTerm)

        if (segment.mayMatch(rangeTermExp)) {
            const remTerms = new Array<Exp>()

            for (let i = fromRem; i < toRem; i++) {
                const p = TermPrefix.NUMBER_L0 + ENCODING_DIGITS[i]
                const termExp = new TermExp(field, p)

                if (segment.mayMatch(termExp)) {
                    remTerms.push(termExp)
                }
            }

            if (remTerms.length > 0) {
                return new BooleanExpression(remTerms, [rangeTermExp])
            }
        }
    } else {
        let rangeTerm = extractNumberTerms(fromRange.shiftLeft((level+1) * 6))[9-level]
        rangeTerm = rangeTerm.substring(0, rangeTerm.length-1)

        const terms = new Array<Exp>()
        for (let range = fromRem; range < toRem; range++) {
            const termExp = new TermExp(field, rangeTerm + ENCODING_DIGITS[range])

            if (segment.mayMatch(termExp)) {
                terms.push(termExp)
            }
        }
        if (terms.length > 0) {
            return new BooleanExpression(terms)
        }
    }

    return new NoneExp()
}

export function addRange(field: FieldName, from: Long, to: Long, level: number, segment: BaseSegment): Exp[] {
    let terms = new Array<Exp>()
    if (from.gte(to)) return terms

    const precision = Long.fromNumber(64)
    const fromRange = from.div(precision)
    const fromRem = from.mod(precision).toNumber()
    const toRange = to.div(precision)
    const toRem = to.mod(precision).toNumber()

    if (fromRem == 0 && fromRange.equals(toRange)) {
        terms = terms.concat(addRange(field, fromRange, fromRange.add(1), level + 1, segment))
    } else {
        if (fromRange.equals(toRange)) {
            terms.push(rangeToExp(field, fromRange, level, fromRem, toRem, segment))
        } else {
            terms.push(rangeToExp(field, fromRange, level, fromRem, 64, segment))
        }
    }

    terms = terms.concat(addRange(field, fromRange.add(1), toRange, level + 1, segment))

    if (toRem != 0 && !fromRange.equals(toRange)) {
        terms.push(rangeToExp(field, toRange, level, 0, toRem, segment))
    }

    return terms.filter(it => !(it instanceof NoneExp))
}


