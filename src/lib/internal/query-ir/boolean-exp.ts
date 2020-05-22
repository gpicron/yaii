import {ALL_EXP, AllExp, Exp, NoneExp} from "./base"
import {MutableSegment} from "../segments/mutable-segment"
import {removeAllFromSet} from "../utils"
import {andMany, andNot, orMany} from "../datastructs/docid-async-iterable/operations"
import {BaseSegment} from "../segments/segment"
import {EMPTY_MAP, RangeDocidAsyncIterable} from "../datastructs/docid-async-iterable/range-docid-async-iterable"
import {DocIdIterable} from "../datastructs/docid-async-iterable/base"


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

    rewrite(segment?: BaseSegment): Exp {
        const should = new Set<Exp>()
        // rewrite should clauses, remove duplicates and bubble up when possible
        if (this.should.length > 0) {
            for (const q of this.should) {
                if (q instanceof BooleanExpression) {
                    const nq = q.rewrite(segment)

                    if (nq instanceof AllExp) {
                        should.clear();
                        should.add(nq);
                        break;
                    } else if (nq instanceof BooleanExpression && nq.isShouldOnly()) {
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
                } else if (q instanceof AllExp) {
                    // nothing to add
                } else if (q instanceof NoneExp) {
                    return q
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
                } else if (q instanceof AllExp) {
                    return new NoneExp()
                } else if (q instanceof NoneExp) {
                    // ignore
                } else {
                    mustNot.add(q)
                }
            }
        }

        // remove should that are also must or mustNot
        if (should.size > 0) {
            removeAllFromSet(should, must)
            removeAllFromSet(should, mustNot)
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
                if (mustNot.has(m)) return new NoneExp()
            }
        }

        // if the query resume to 1 must clause, bubble up
        if (must.size == 1 && should.size == 0 && mustNot.size == 0) {
            return must.values().next().value
        }


        if (must.size == 0 && should.size == 0 && mustNot.size == 0) {
            return ALL_EXP
        }

        return new BooleanExpression(Array.from(should), Array.from(must), Array.from(mustNot))
    }

    async resolve(segment: MutableSegment, forceResolveToBitmap: boolean = false): Promise<DocIdIterable> {
        const promiseMust = 
            this.must.map( async oper => oper.resolve(segment, true))
        

        const resolvedMust = new Array<DocIdIterable>()
        for  (const pm of promiseMust) {
            const m = await pm
            if (m.cost == 0) return EMPTY_MAP
            resolvedMust.push(m)
        }

        const resolvedMustNots = (await Promise.all(
            this.mustNot.map(async oper => oper.resolve(segment, true))
        )).filter(oper => oper.cost > 0)

        const must = resolvedMust.length === 0 ? undefined : andMany(resolvedMust)
        const mustNot = orMany(resolvedMustNots)

        const should = this.should.map(async (exp: Exp) => exp.resolve(segment, forceResolveToBitmap))

        if (should.length == 0) {
            if (must) {
                return must
            } else if (mustNot) {
                return andNot(new RangeDocidAsyncIterable(0, segment.rangeSize), mustNot)
            } else {
                throw new Error('bug')
            }
        } else if (should.length == 1) {
            throw new Error('bug')
        } else {
            const allShoulds = orMany(await Promise.all(should))

            if (must) {
                const a = andMany([must, allShoulds])
                if (a) {
                    return andNot(a, mustNot)
                } else {
                    throw new Error("bug")
                }
            } else {
                return andNot(allShoulds, mustNot)
            }
        }
    }
}
