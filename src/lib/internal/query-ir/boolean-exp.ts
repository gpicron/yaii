import {ALL_EXP, Exp} from "./base"
import {MutableSegment} from "../mutable-segment"
import {removeAll} from "../utils"
import {DocidAsyncIterable, orMany} from "../datastructs/docid-async-iterable/docid-async-iterable"
import {BitmapDocidAsyncIterable} from "../datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {
    cloneIfNotReusable,
    SingletonDocidAsyncIterable
} from "../datastructs/docid-async-iterable/singleton-docid-async-iterable"



export class BooleanExpression implements Exp {
    should: Array<Exp>
    must: Array<Exp>
    mustNot: Array<Exp>

    constructor(should?: Exp[], must?: Exp[], mustNot?: Exp[]) {
        this.should = should || new Array<Exp>()
        this.must = must || new Array<Exp>()
        this.mustNot = mustNot || new Array<Exp>()
    }

    isShouldOnly(): boolean {
        return this.must.length + this.mustNot.length == 0
    }

    isMustOnly(): boolean {
        return this.should.length + this.mustNot.length == 0
    }

    toString(): string {
        let result = '('
        result += this.should.map(value => `(${value})`).join(' ')
        result += ' '
        result += this.must.map(value => `+${value}`).join(' ')
        result += ' '
        result += this.mustNot.map(value => `!${value}`).join(' ')
        result += ')'

        return result
    }

    rewrite(segment?: MutableSegment): Exp {
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
        const mustToAnalyze: Array<Exp> = Array.from(this.must)
        if (should.size == 1) {
            mustToAnalyze.push(should.values().next().value as Exp)
            should.clear()
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

    async resolve(segment: MutableSegment, forceResolveToBitmap: boolean = false): Promise<DocidAsyncIterable> {
        const resolvedMust = this.must.map(async oper => oper.resolve(segment, true))

        const mapMusts = new Array<DocidAsyncIterable>()
        const lazyMusts = new Array<DocidAsyncIterable>()

        for await (const m of resolvedMust) {
            if (BitmapDocidAsyncIterable.is(m) || SingletonDocidAsyncIterable.is(m)) {
                if (m.size == 0) return BitmapDocidAsyncIterable.EMPTY_MAP
                mapMusts.push(m)
            } else {
                lazyMusts.push(m)
            }
        }

        const resolvedMustNots = this.mustNot.map(async oper => oper.resolve(segment, true))

        const mapMustNots = new Array<DocidAsyncIterable>()
        const lazyMustNots = new Array<DocidAsyncIterable>()

        for await (const m of resolvedMustNots) {
            if (BitmapDocidAsyncIterable.is(m) || SingletonDocidAsyncIterable.is(m)) {
                if (m.size > 0) mapMustNots.push(m)
            } else {
                lazyMustNots.push(m)
            }
        }

        let must: BitmapDocidAsyncIterable | undefined
        let mustNot: BitmapDocidAsyncIterable | undefined

        if (resolvedMust.length > 0) {
            if (mapMusts.length > 0) {
                mapMusts.sort((a, b) => a.size - b.size)
                const first = mapMusts.shift() as DocidAsyncIterable
                must = cloneIfNotReusable(first)

                // first, and(all must bitmaps)
                for (const nextMust of mapMusts) {
                    must.andInPlace(nextMust)
                    if (must.size == 0) return BitmapDocidAsyncIterable.EMPTY_MAP
                }
                // and remove all mustNots
                for (const nextMustNot of mapMustNots) {
                    must.andNotInPlace(nextMustNot)
                    if (must.size == 0) return BitmapDocidAsyncIterable.EMPTY_MAP
                }
            }

            if (lazyMusts.length > 0) {
                if (must == undefined) {
                    const first = lazyMusts.shift() as DocidAsyncIterable

                    must = new BitmapDocidAsyncIterable()
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

                    if (must.size == 0) return BitmapDocidAsyncIterable.EMPTY_MAP
                }
                // and remove all mustNots
                for (const nextMustNot of lazyMustNots) {
                    for await (const index of nextMustNot) must.remove(index)

                    if (must.size == 0) return BitmapDocidAsyncIterable.EMPTY_MAP
                }
            }
        } else if (resolvedMustNots.length > 0) {
            mustNot = orMany(mapMustNots)

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

                const singletons = new Array<SingletonDocidAsyncIterable>()
                const bitmaps = new Array<BitmapDocidAsyncIterable>()

                for (const didai of allShouldBitmaps) {
                    if (didai.size > 0) {
                        if (BitmapDocidAsyncIterable.is(didai)) {
                            bitmaps.push(didai)
                        } else {
                            singletons.push(didai as SingletonDocidAsyncIterable)
                        }
                    }
                }

                let result
                if (bitmaps.length > 0) {
                    result = orMany(bitmaps)
                } else {
                    result = new BitmapDocidAsyncIterable()
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
                        if (BitmapDocidAsyncIterable.is(value)) {
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
