import {DocId} from "../../../api/base"
import {DocIdIterable, DocIdIterator, NO_MORE_DOC} from "./base"


export class SingletonDocidAsyncIterable extends DocIdIterable {
    readonly mutable: boolean = false
    readonly cost: number = 1
    readonly index: number
    readonly sizeInMemory: number = 16

    constructor(index: number) {
        super()
        this.index = index
    }

    static is(x: DocIdIterable): x is SingletonDocidAsyncIterable {
        return x instanceof SingletonDocidAsyncIterable
    }

    has(e: DocId): boolean {
        return this.index === e
    }

    [Symbol.iterator](): DocIdIterator {
        const singleton = this.index
        let done = false
        return {
            next(skipUntil?: DocId): IteratorResult<DocId, unknown> {
                if (done) return NO_MORE_DOC
                if (skipUntil && skipUntil > singleton) return NO_MORE_DOC
                done = true
                return {
                    value: singleton,
                    done: false
                }
            }
        };
    }



}
