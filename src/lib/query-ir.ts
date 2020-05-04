import { as, toArray } from 'ix/asynciterable'
import { RoaringBitmap32 } from 'roaring'
import {
    AndQuery,
    FieldName,
    FieldPresentQuery,
    LeafQuery,
    NotQuery,
    NumberQuery,
    NumberRangeQuery,
    OrQuery,
    Query,
    QueryOperator,
    TokenQuery
} from '../yaii-types'

import { IndexSegment } from './index-segment'
import { removeAll } from './utils'
import { BitmapAsyncIterable, DocIdAsyncIterable, SingletonDocIdAsyncIterable } from './bitmap'
import * as Long from 'long'

export enum INTERNAL_FIELDS {
    FIELDS = '£_FIELD',
    ALL = '£_ALL',
    SOURCE = '£_SOURCE'
}

export abstract class Exp {
    // eslint-disable-next-line
    rewrite(segment: IndexSegment): Exp {
        return this
    }

    abstract async resolve(segment: IndexSegment, forceResolveToBitmap?: boolean): Promise<DocIdAsyncIterable>
}

export class AllExp extends Exp {
    async resolve(segment: IndexSegment): Promise<DocIdAsyncIterable> {
        const m = RoaringBitmap32.fromRange(segment.from, segment.size + segment.from)
        return new BitmapAsyncIterable(m, true)
    }

    toString() {
        return 'ALL'
    }
}

const ALL_EXP = new AllExp()

export class NoneExp extends Exp {
    async resolve(): Promise<DocIdAsyncIterable> {
        return new BitmapAsyncIterable(new RoaringBitmap32(), true)
    }

    toString() {
        return 'NONE'
    }
}

export type Term = Buffer

export class TermExp extends Exp {
    readonly field: FieldName
    readonly term: Term

    constructor(field: FieldName, term: Term) {
        super()
        this.field = field
        this.term = term
    }

    toString() {
        return `${this.field}:${this.term.toString('hex')}`
    }

    async resolve(segment: IndexSegment): Promise<DocIdAsyncIterable> {
        const docIdAsyncIterable = segment.get(this.field, this.term)
        return docIdAsyncIterable
    }
}

export class BooleanExpression implements Exp {
    should: Array<Exp>
    must: Array<Exp>
    mustNot: Array<Exp>

    constructor(should?: Exp[], must?: Exp[], mustNot?: Exp[]) {
        this.should = should || new Array<Exp>()
        this.must = must || new Array<Exp>()
        this.mustNot = mustNot || new Array<Exp>()
    }

    isShouldOnly() {
        return this.must.length + this.mustNot.length == 0
    }

    isMustOnly() {
        return this.should.length + this.mustNot.length == 0
    }

    toString() {
        let result = '('
        result += this.should.map(value => `(${value})`).join(' ')
        result += ' '
        result += this.must.map(value => `+${value}`).join(' ')
        result += ' '
        result += this.mustNot.map(value => `!${value}`).join(' ')
        result += ')'

        return result
    }

    rewrite(segment?: IndexSegment): Exp {
        const should = new Set<Exp>()
        // rewrite should clauses, remove duplicates and bubble up when possible
        if (this.should.length > 0) {
            for (const q of this.should) {
                if (q instanceof BooleanExpression) {
                    const nq = q.rewrite(segment)

                    if (nq instanceof BooleanExpression && nq.isShouldOnly()) {
                        q.should.forEach(value => should.add(value))
                    } else {
                        should.add(nq)
                    }
                } else {
                    should.add(q)
                }
            }
        }

        // if only 1 should, become a must
        let mustToAnalyze: Array<Exp>
        if (should.size == 1 && this.isShouldOnly()) {
            mustToAnalyze = Array.from(should.values())
            should.clear()
        } else {
            mustToAnalyze = this.must
        }

        // rewrite must clauses, remove duplicates and bubble up when possible
        const must = new Set<Exp>()
        if (mustToAnalyze.length > 0) {
            for (const q of mustToAnalyze) {
                if (q instanceof BooleanExpression) {
                    const nq = q.rewrite(segment)

                    if (nq instanceof BooleanExpression && nq.isMustOnly()) {
                        q.must.forEach(value => must.add(value))
                    } else {
                        must.add(nq)
                    }
                } else {
                    must.add(q)
                }
            }
        }

        // rewrite mustNot clauses, remove duplicates and bubble up when possible
        const mustNot = new Set<Exp>()
        if (this.mustNot.length > 0) {
            for (const q of this.mustNot) {
                if (q instanceof BooleanExpression) {
                    const nq = q.rewrite(segment)

                    if (nq instanceof BooleanExpression && nq.isShouldOnly()) {
                        q.should.forEach(value => mustNot.add(value))
                    } else {
                        mustNot.add(nq)
                    }
                } else {
                    mustNot.add(q)
                }
            }
        }

        // remove should that are also must or mustNot
        if (should.size > 0) {
            removeAll(should, must)
            removeAll(should, mustNot)
        }

        // bubble up must(mustNot) ==> mustNot
        if (must.size > 0) {
            ;[...must].forEach(m => {
                if (m instanceof BooleanExpression && m.must.length == 0 && m.should.length == 0) {
                    ;[...m.mustNot].forEach(n => mustNot.add(n))
                    must.delete(m)
                }
            })
        }

        // if there is same in must and mustNot return None
        if (mustNot.size > 0) {
            for (const m of must) {
                if (mustNot.has(m)) return ALL_EXP
            }
        }

        // if the query resume to 1 must clause, bubble up
        if (must.size == 1 && should.size == 0 && mustNot.size == 0) {
            return must.values().next().value
        }

        // if the query resume to 1 must clause, bubble up
        if (must.size == 0 && should.size == 0 && mustNot.size == 0) {
            return ALL_EXP
        }

        return new BooleanExpression(Array.from(should), Array.from(must), Array.from(mustNot))
    }

    async resolve(segment: IndexSegment, forceResolveToBitmap: boolean = false): Promise<DocIdAsyncIterable> {
        const resolvedMust = this.must.map(async oper => oper.resolve(segment, true))

        const mapMusts = new Array<DocIdAsyncIterable>()
        const lazyMusts = new Array<DocIdAsyncIterable>()

        for await (const m of resolvedMust) {
            if (BitmapAsyncIterable.is(m) || SingletonDocIdAsyncIterable.is(m)) {
                mapMusts.push(m)
            } else {
                lazyMusts.push(m)
            }
        }

        const resolvedMustNots = this.mustNot.map(async oper => oper.resolve(segment, true))

        const mapMustNots = new Array<DocIdAsyncIterable>()
        const lazyMustNots = new Array<DocIdAsyncIterable>()

        for await (const m of resolvedMustNots) {
            if (BitmapAsyncIterable.is(m) || SingletonDocIdAsyncIterable.is(m)) {
                mapMustNots.push(m)
            } else {
                lazyMustNots.push(m)
            }
        }

        let must: BitmapAsyncIterable | undefined
        let mustNot: BitmapAsyncIterable | undefined

        if (resolvedMust.length > 0) {
            if (mapMusts.length > 0) {
                mapMusts.sort((a, b) => a.size - b.size)
                const first = mapMusts.shift() as DocIdAsyncIterable
                if (first.canUpdateInPlace) {
                    must = first as BitmapAsyncIterable
                } else {
                    must = first.clone()
                }

                // first, and(all must bitmaps)
                for (const nextMust of mapMusts) {
                    must.andInPlace(nextMust)
                    if (must.size == 0) return BitmapAsyncIterable.EMPTY_MAP
                }
                // and remove all mustNots
                for (const nextMustNot of mapMustNots) {
                    must.andNotInPlace(nextMustNot)
                    if (must.size == 0) return BitmapAsyncIterable.EMPTY_MAP
                }
            }

            if (lazyMusts.length > 0) {
                if (must == undefined) {
                    const first = lazyMusts.shift() as DocIdAsyncIterable

                    must = new BitmapAsyncIterable(new RoaringBitmap32(), true)
                    for await (const index of first) must.add(index)
                }

                // first, and(all must bitmaps)
                for (const nextMust of lazyMusts) {
                    let low = 0
                    for await (const index of nextMust) {
                        const high = index

                        if (low < high) must.removeRange(low, high)
                        low = high + 1
                    }
                    must.removeRange(low, 4294967297)

                    if (must.size == 0) return BitmapAsyncIterable.EMPTY_MAP
                }
                // and remove all mustNots
                for (const nextMustNot of lazyMustNots) {
                    for await (const index of nextMustNot) must.remove(index)

                    if (must.size == 0) return BitmapAsyncIterable.EMPTY_MAP
                }
            }
        } else if (resolvedMustNots.length > 0) {
            mustNot = BitmapAsyncIterable.orMany(mapMustNots)

            for (const nextMustNot of lazyMustNots) {
                for await (const index of nextMustNot) mustNot.add(index)
            }
        }

        /// ----- Lazy resolve not optimal for now, force resolve
        forceResolveToBitmap = true
        /// ---

        const should = this.should.map(async (exp: Exp) => exp.resolve(segment, forceResolveToBitmap))

        if (should.length == 0) {
            if (must) {
                return must
            } else if (mustNot) {
                mustNot.flipRange(segment.from, segment.next)

                return mustNot
            } else {
                throw new Error('bug')
            }
        } else if (should.length == 1) {
            throw new Error('bug')
        } else {
            if (forceResolveToBitmap || true) {
                const allShouldBitmaps = await Promise.all(should)

                const singletons = new Array<SingletonDocIdAsyncIterable>()
                const bitmaps = new Array<BitmapAsyncIterable>()

                for (const didai of allShouldBitmaps) {
                    if (BitmapAsyncIterable.is(didai)) {
                        bitmaps.push(didai)
                    } else {
                        singletons.push(didai as SingletonDocIdAsyncIterable)
                    }
                }

                let result
                if (bitmaps.length > 0) {
                    result = BitmapAsyncIterable.orMany(bitmaps)
                } else {
                    result = new BitmapAsyncIterable(new RoaringBitmap32(), true)
                }

                for (const singleton of singletons) {
                    result.add(singleton.index)
                }

                if (must) {
                    return result.andInPlace(must)
                } else if (mustNot) {
                    return result.andNotInPlace(mustNot)
                } else {
                    return result
                }
            } else {
                // TODO must implement a parallel traversal feature here.  This version provides ids out of order
                // and with duplicate.  For now, we for resolve to bitmap first
                /*const result = as(should).pipe(
                    op.flatMap(async value => {
                        if (BitmapAsyncIterable.is(value)) {
                            if (must) {
                                return cloneIfNotReusable(
                                    await value
                                ).andInPlace(must)
                            } else if (mustNot) {
                                return cloneIfNotReusable(
                                    await value
                                ).andNotInPlace(mustNot)
                            } else {
                                return value
                            }
                        } else return value
                    })
                )
*/
                throw new Error('not yet implemented')
            }
        }
    }
}

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

export const stringToTerm = (token: string) => {
    const data = Buffer.from(token, 'utf8')
    const result = Buffer.allocUnsafe(data.length + 1)

    result[0] = TermPrefix.BUFFER_OR_STRING
    data.copy(result, 1)

    return result
}

export function numberToTerms(v: number) {
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

export function numberToTerms2(v: number) {
    const temp = Buffer.allocUnsafe(8)
    let bint = BigInt(Math.floor(v))
    bint += BigInt(Number.MAX_SAFE_INTEGER) // shift to positive number

    temp.writeBigUInt64BE(bint)

    const result = []

    for (let i = 0; i < 6; i++) {
        const t = Buffer.allocUnsafe(i + 2)
        t.writeUInt8(TermPrefix.NUMBER_L6 - i, 0)
        temp.copy(t, 1, 1, i + 2)
        result.push(t)
    }

    result.push(Buffer.of(TermPrefix.NUMBER_L0, temp.readUInt8(7)))

    return result
}

function fieldNameOrAll(q: LeafQuery) {
    return q.field || INTERNAL_FIELDS.ALL
}

export async function buildExpression(query: Query): Promise<Exp> {
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
            const operands = await toArray(as(q.operands.map(buildExpression)))

            return new BooleanExpression(undefined, operands)
        }
        case QueryOperator.OR: {
            const q = query as OrQuery

            const operands = await toArray(as(q.operands.map(buildExpression)))

            return new BooleanExpression(operands)
        }
        case QueryOperator.NOT: {
            const q = query as NotQuery

            return new BooleanExpression(undefined, undefined, [await buildExpression(q.operand)])
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
