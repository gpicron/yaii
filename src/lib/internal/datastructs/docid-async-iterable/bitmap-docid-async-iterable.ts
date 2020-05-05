import {AsyncIterableX} from "ix/asynciterable"
import {DocId} from "../../../api/base"
import {DocidAsyncIterable} from "./docid-async-iterable"
import {RoaringBitmap32} from "roaring"
import {SingletonDocidAsyncIterable} from "./singleton-docid-async-iterable"

export class BitmapDocidAsyncIterable extends AsyncIterableX<DocId> implements DocidAsyncIterable {
    readonly bitmap: RoaringBitmap32
    readonly canUpdateInPlace: boolean

    constructor(canUpdateInPlace?: boolean, bitmap?: RoaringBitmap32) {
        super()
        this.bitmap = bitmap || new RoaringBitmap32()
        this.canUpdateInPlace = canUpdateInPlace || true
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
        this.bitmap.add(index)
        return this
    }

    remove(index: number): this {
        this.bitmap.remove(index)
        return this
    }

    removeRange(low: number, high: number = 4294967297): this {
        this.bitmap.removeRange(low, high)
        return this
    }

    andInPlace(and: DocidAsyncIterable): this {
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
        if (BitmapDocidAsyncIterable.is(andNot)) {
            this.bitmap.andNotInPlace(andNot.bitmap)
        } else {
            const andNotSingleton = andNot as SingletonDocidAsyncIterable
            this.bitmap.remove(andNotSingleton.index)
        }
        return this
    }

    flipRange(from: number, to: number): this {
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
}

export function orManyBitmap(opers: BitmapDocidAsyncIterable[]): BitmapDocidAsyncIterable {
    return new BitmapDocidAsyncIterable(true, RoaringBitmap32.orMany(opers.map(it => it.bitmap)))
}
