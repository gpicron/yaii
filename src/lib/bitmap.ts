import { AsyncIterableX } from 'ix/asynciterable'
import { RoaringBitmap32 } from 'roaring'

export declare abstract class DocIdAsyncIterable extends AsyncIterableX<number> {
    canUpdateInPlace: boolean
    readonly size: number

    clone(): BitmapAsyncIterable

    add(index: number): this

    remove(index: number): this

    removeRange(low: number, high: number): this

    andInPlace(and: BitmapAsyncIterable): this

    andNotInPlace(andNot: BitmapAsyncIterable): this

    flipRange(from: number, to: number): this

    [Symbol.asyncIterator](): AsyncIterator<number>
}

export class BitmapAsyncIterable extends AsyncIterableX<number> implements DocIdAsyncIterable {
    private bitmap: RoaringBitmap32
    readonly canUpdateInPlace: boolean

    constructor(bitmap: RoaringBitmap32, canUpdateInPlace: boolean) {
        super()
        this.bitmap = bitmap
        this.canUpdateInPlace = canUpdateInPlace
    }

    static is(x: AsyncIterable<number>): x is BitmapAsyncIterable {
        return x instanceof BitmapAsyncIterable
    }

    get size(): number {
        return this.bitmap.size
    }

    clone(): BitmapAsyncIterable {
        return new BitmapAsyncIterable(this.bitmap.clone(), true)
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

    andInPlace(and: DocIdAsyncIterable): this {
        if (BitmapAsyncIterable.is(and)) {
            this.bitmap.andInPlace(and.bitmap)
        } else {
            const andSingleton = and as SingletonDocIdAsyncIterable
            if (this.bitmap.has(andSingleton.index)) {
                this.bitmap.clear()
                this.bitmap.add(andSingleton.index)
            } else {
                this.bitmap.clear()
            }
        }
        return this
    }

    andNotInPlace(andNot: DocIdAsyncIterable): this {
        if (BitmapAsyncIterable.is(andNot)) {
            this.bitmap.andNotInPlace(andNot.bitmap)
        } else {
            const andNotSingleton = andNot as SingletonDocIdAsyncIterable
            this.bitmap.remove(andNotSingleton.index)
        }
        return this
    }

    static orMany(opers: DocIdAsyncIterable[]) {
        let result: RoaringBitmap32 | undefined
        const maps = opers.filter(BitmapAsyncIterable.is).map(v => v.bitmap)
        if (maps.length > 0) {
            result = RoaringBitmap32.orMany(maps)
        }
        if (maps.length < opers.length) {
            if (!result) result = new RoaringBitmap32()

            for (const s of opers) {
                // eslint-disable-next-line
                if (SingletonDocIdAsyncIterable.is(s)) {
                    result.add(s.index)
                }
            }
        }

        return new BitmapAsyncIterable(result as RoaringBitmap32, true)
    }

    flipRange(from: number, to: number): this {
        this.bitmap.flipRange(from, to)
        return this
    }

    async *[Symbol.asyncIterator](): AsyncIterator<number> {
        for (const item of this.bitmap) {
            yield item
        }
    }

    static EMPTY_MAP = new BitmapAsyncIterable(new RoaringBitmap32(), false)
}

export class SingletonDocIdAsyncIterable extends AsyncIterableX<number> implements DocIdAsyncIterable {
    readonly canUpdateInPlace: boolean = false
    readonly size: number = 1
    readonly index: number

    constructor(index: number) {
        super()
        this.index = index
    }

    static is(x: AsyncIterable<number>): x is SingletonDocIdAsyncIterable {
        return x instanceof SingletonDocIdAsyncIterable
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

    clone(): BitmapAsyncIterable {
        return new BitmapAsyncIterable(new RoaringBitmap32([this.index]), true)
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

    async *[Symbol.asyncIterator](): AsyncIterator<number> {
        yield this.index
    }
}

export function cloneIfNotReusable(value: BitmapAsyncIterable): BitmapAsyncIterable {
    if (!value.canUpdateInPlace) return value.clone()
    return value
}
