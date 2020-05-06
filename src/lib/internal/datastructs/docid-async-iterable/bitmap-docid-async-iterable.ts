import {AsyncIterableX} from "ix/asynciterable"
import {DocId} from "../../../api/base"
import {DocidAsyncIterable} from "./docid-async-iterable"
import {RoaringBitmap32} from "roaring"
import {cloneIfNotReusable, SingletonDocidAsyncIterable} from "./singleton-docid-async-iterable"

export class BitmapDocidAsyncIterable extends AsyncIterableX<DocId> implements DocidAsyncIterable {
    private bitmap: RoaringBitmap32
    readonly mutable: boolean

    constructor(mutable: boolean = true, bitmap?: RoaringBitmap32) {
        super()
        this.bitmap = bitmap || new RoaringBitmap32()
        this.mutable = mutable
    }

    static is(x: AsyncIterable<number>): x is BitmapDocidAsyncIterable {
        return x instanceof BitmapDocidAsyncIterable
    }

    get size(): number {
        return this.bitmap.size
    }

    clone(): BitmapDocidAsyncIterable {
        return new BitmapDocidAsyncIterable(true, this.bitmap.clone())
    }

    add(index: number): this {
        if (!this.mutable) throw new Error('Immutable')

        this.bitmap.add(index)
        return this
    }

    remove(index: number): this {
        if (!this.mutable) throw new Error('Immutable')

        this.bitmap.remove(index)
        return this
    }

    removeRange(low: number, high: number = 4294967297): this {
        if (!this.mutable) throw new Error('Immutable')

        this.bitmap.removeRange(low, high)
        return this
    }

    andInPlace(and: DocidAsyncIterable): this {
        if (!this.mutable) throw new Error('Immutable')

        if (BitmapDocidAsyncIterable.is(and)) {
            this.bitmap.andInPlace(and.bitmap)
        } else {
            const andSingleton = and as SingletonDocidAsyncIterable
            if (this.bitmap.has(andSingleton.index)) {
                this.bitmap.clear()
                this.bitmap.add(andSingleton.index)
            } else {
                this.bitmap.clear()
            }
        }
        return this
    }

    andNotInPlace(andNot: DocidAsyncIterable): this {
        if (!this.mutable) throw new Error('Immutable')

        if (BitmapDocidAsyncIterable.is(andNot)) {
            this.bitmap.andNotInPlace(andNot.bitmap)
        } else {
            const andNotSingleton = andNot as SingletonDocidAsyncIterable
            this.bitmap.remove(andNotSingleton.index)
        }
        return this
    }

    flipRange(from: number, to: number): this {
        if (!this.mutable) throw new Error('Immutable')

        this.bitmap.flipRange(from, to)
        return this
    }

    async* [Symbol.asyncIterator](): AsyncIterator<number> {
        for (const item of this.bitmap) {
            yield item
        }
    }

    static EMPTY_MAP = new BitmapDocidAsyncIterable(false, new RoaringBitmap32())

    has(e: DocId): boolean {
        return this.bitmap.has(e);
    }

    static orManyBitmap(opers: BitmapDocidAsyncIterable[]): BitmapDocidAsyncIterable {
        if (opers.length === 0) return BitmapDocidAsyncIterable.EMPTY_MAP
        if (opers.length === 1) return cloneIfNotReusable(opers[0])

        return new BitmapDocidAsyncIterable(true, RoaringBitmap32.orMany(opers.map(it => it.bitmap)))
    }

}

