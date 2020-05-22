import {DocId} from "../../../api/base"
import {DocIdIterable, DocIdIterator, NO_MORE_DOC} from "./base"

export class RangeDocidAsyncIterable extends DocIdIterable {
    readonly mutable: boolean = false
    readonly cost: number
    readonly sizeInMemory: number = 16
    private from: DocId

    constructor(from: DocId, size: number) {
        super()
        this.from = from
        this.cost = size
    }

    static is(x: DocIdIterable): x is RangeDocidAsyncIterable {
        return x instanceof RangeDocidAsyncIterable
    }

    has(e: DocId): boolean {
        const rel = e - this.from
        return rel > 0 && rel < this.cost
    }

    [Symbol.iterator](): DocIdIterator {
        const current = {
            value: this.from-1,
            done: false
        }

        const end = this.cost + this.from
        return {
            next: function(skipUntil?: DocId) {
                if (skipUntil) {
                    if (skipUntil < current.value) throw new Error("skipUntil lower than current")
                    current.value = skipUntil
                } else {
                    current.value++
                }

                if (current.value >= end) {
                    return NO_MORE_DOC
                } else {
                    return current
                }
            }
        };
    }




}


export const EMPTY_MAP = new RangeDocidAsyncIterable(0,0)
