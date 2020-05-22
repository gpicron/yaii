import {DocIdIterable, DocIdIterator, NO_MORE_DOC} from "./base"
import {EMPTY_MAP} from "./range-docid-async-iterable"
import {DocId} from "../../../api/base"
import Heap from "../binary-heap"
import assert = require("assert")

type IterAndNext = {
    iter: DocIdIterator,
    next: number
}

class DisjonctionDocIdIterable extends DocIdIterable {
    readonly cost: number
    readonly mutable = false
    readonly sizeInMemory: number = 16

    readonly opers: DocIdIterable[]

    constructor(opers: DocIdIterable[]) {
        super()
        this.opers = opers.sort((a, b) => b.cost - a.cost);
        this.cost = opers.reduce((previousValue, currentValue) => previousValue + currentValue.cost, 0)
    }

    has(e: DocId): boolean {
        for (const o of this.opers) {
            if (o.has(e)) return true
        }

        return false
    }

    [Symbol.iterator](): DocIdIterator {
        const heap = new Heap<IterAndNext>((a, b) => a.next - b.next);
        const current = {
            value: -1,
            done: false
        }

        let min = 0
        for (const op of this.opers) {
            const iter = op[Symbol.iterator]()
            const docIdIteratorResult = iter.next(min)
            if (!docIdIteratorResult.done) {
                min =  docIdIteratorResult.value
                heap.add({
                    iter: iter,
                    next: docIdIteratorResult.value
                })
            }
        }

        return {
            next(skipUntil?: DocId): IteratorResult<DocId> {
                skipUntil = skipUntil || current.value + 1

                let root = heap.peek()
                while (root && root.next < skipUntil) {
                    const next = root.iter.next(skipUntil)
                    if (next.done) {
                        heap.removeRoot()
                    } else {
                        root.next = next.value
                        heap.siftDown(0)
                    }
                    root = heap.peek()
                }

                if (root) {
                    current.value =  root.next

                    return current
                } else {
                    return NO_MORE_DOC
                }

            }
        }

    }


}

export function orMany(opers: DocIdIterable[]): DocIdIterable {
    opers = opers.reduce((all: DocIdIterable[], oper: DocIdIterable) => {
        if (oper instanceof DisjonctionDocIdIterable) {
            all.push(...oper.opers)
        } else {
            all.push(oper)
        }
        return all
    }, [] )

    opers = opers.filter(o => o.cost > 0)
    if (opers.length === 1) return opers[0]
    if (opers.length === 0) return EMPTY_MAP

    return new DisjonctionDocIdIterable(opers)
}



class ConjonctionDocIdIterable extends DocIdIterable {
    readonly mutable = false
    readonly sizeInMemory: number = 16
    readonly cost: number

    opers: DocIdIterable[]

    constructor(opers: DocIdIterable[]) {
        super()
        this.opers = opers.sort((a, b) => a.cost - b.cost);
        this.cost = this.opers[0].cost
    }

    has(e: DocId): boolean {
        for (const o of this.opers) {
            if (!o.has(e)) return false
        }

        return true
    }

    [Symbol.iterator](): DocIdIterator {
        const iterators = this.opers.map(o => o[Symbol.iterator]())
        assert (iterators.length > 1)

        const lead = iterators.shift() as DocIdIterator

        const next: IteratorResult<DocId, unknown> = {
            value: -1,
            done: false
        }

        return {
            next(skipUntil?: DocId): IteratorResult<DocId> {
                let nextLead = lead.next(skipUntil);

                while (!nextLead.done) {
                    const nextDocId = nextLead.value
                    const others = iterators.map(i => i.next(nextDocId))

                    let nextPotential = nextDocId
                    for (const o of others) {
                        if (o.done) return NO_MORE_DOC

                        nextPotential = Math.max(nextPotential, o.value)
                    }

                    if (nextPotential == nextDocId) {
                        next.value = nextDocId
                        return next
                    }

                    nextLead = lead.next(nextPotential)
                }

                return NO_MORE_DOC
            }
        }

    }
}

export function andMany(opers: DocIdIterable[]): DocIdIterable {
    assert(opers.length > 0)
    opers = opers.reduce((all: DocIdIterable[], oper: DocIdIterable) => {
        if (oper instanceof ConjonctionDocIdIterable) {
            all.push(...oper.opers)
        } else {
            all.push(oper)
        }
        return all
    }, [] )

    if (opers.some(o => o.cost == 0)) return EMPTY_MAP

    if (opers.length === 1) return opers[0]

    return new ConjonctionDocIdIterable(opers)
}

class AndNotDocIdIterable extends DocIdIterable {
    readonly cost: number
    readonly mutable = false
    readonly sizeInMemory: number = 16
    private a: DocIdIterable
    private not: DocIdIterable


    constructor(a: DocIdIterable, not: DocIdIterable) {
        super()
        this.a = a
        this.not = not
        this.cost = a.cost
    }

    has(e: DocId): boolean {
        return this.a.has(e) && !this.not.has(e)
    }

    [Symbol.iterator](): DocIdIterator {
        const iterator = this.a[Symbol.iterator]()
        const not = this.not

        return {
            next(skipUntil?: DocId): IteratorResult<DocId> {
                let next = iterator.next(skipUntil);

                while (!next.done && not.has(next.value)) {
                    next = iterator.next();
                }

                return next
            }
        }

    }
}

export function andNot(a: DocIdIterable, not: DocIdIterable): DocIdIterable {
    if (not.cost == 0) return a

    return new AndNotDocIdIterable(a, not)
}
