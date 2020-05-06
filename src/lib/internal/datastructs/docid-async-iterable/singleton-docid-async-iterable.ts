import {AsyncIterableX} from 'ix/asynciterable'
import {RoaringBitmap32} from 'roaring'
import {DocidAsyncIterable} from "./docid-async-iterable"
import {BitmapDocidAsyncIterable} from "./bitmap-docid-async-iterable"
import {DocId} from "../../../api/base"

export class SingletonDocidAsyncIterable extends AsyncIterableX<number> implements DocidAsyncIterable {
    readonly mutable: boolean = false
    readonly size: number = 1
    readonly index: number

    constructor(index: number) {
        super()
        this.index = index
    }

    static is(x: AsyncIterable<number>): x is SingletonDocidAsyncIterable {
        return x instanceof SingletonDocidAsyncIterable
    }

    add(): this {
        throw new Error('Immutable')
    }

    andInPlace(): this {
        throw new Error('Immutable')
    }

    andNotInPlace(): this {
        throw new Error('Immutable')
    }

    clone(): BitmapDocidAsyncIterable {
        return new BitmapDocidAsyncIterable(true, new RoaringBitmap32([this.index]))
    }

    flipRange(): this {
        throw new Error('Immutable')
    }

    remove(): this {
        throw new Error('Immutable')
    }

    removeRange(): this {
        throw new Error('Immutable')
    }

    has(e: DocId): boolean {
        return this.index === e
    }

    async *[Symbol.asyncIterator](): AsyncIterator<number> {
        yield this.index
    }
}

export function cloneIfNotReusable<T extends DocidAsyncIterable>(value: DocidAsyncIterable): BitmapDocidAsyncIterable {
    if (!value.mutable) return value.clone()
    return value as BitmapDocidAsyncIterable
}
