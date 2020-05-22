import {DocId} from "../../../api/base"
import {IterableX} from "ix/iterable"
import {ValueWithMemoryEstimation} from "../lru-cache"

export interface DocIdIterator extends Iterator<DocId>{
    next(skipUntil?: DocId): IteratorResult<DocId>;
}

export const NO_MORE_DOC: IteratorResult<DocId> =  {
    value: undefined,
    done:true
}

export abstract class DocIdIterable extends IterableX<DocId> implements ValueWithMemoryEstimation {
    readonly abstract mutable: boolean
    readonly abstract cost: number
    readonly abstract sizeInMemory: number

    abstract has(e: DocId): boolean

    abstract [Symbol.iterator](): DocIdIterator

}
