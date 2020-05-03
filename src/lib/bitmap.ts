import { AsyncIterableX } from 'ix/asynciterable'
import { RoaringBitmap32 } from 'roaring'

export declare abstract class DocIdAsyncIterable extends AsyncIterableX<
    number
> {
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

export class BitmapAsyncIterable extends AsyncIterableX<number>
    implements DocIdAsyncIterable {
    private bitmap: RoaringBitmap32
    readonly canUpdateInPlace: boolean

    constructor(bitmap: RoaringBitmap32, canUpdateInPlace: boolean) {
        super()
        this.bitmap = bitmap
        this.canUpdateInPlace = canUpdateInPlace
    }

    static is(x: AsyncIterableX<number>): x is BitmapAsyncIterable {
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

    andInPlace(and: BitmapAsyncIterable): this {
        this.bitmap.andInPlace(and.bitmap)
        return this
    }

    andNotInPlace(andNot: BitmapAsyncIterable): this {
        this.bitmap.andNotInPlace(andNot.bitmap)
        return this
    }

    static orMany(opers: BitmapAsyncIterable[]) {
        return new BitmapAsyncIterable(
            RoaringBitmap32.orMany(opers.map(value => value.bitmap)),
            true
        )
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

export class SingletonDocIdAsyncIterable extends AsyncIterableX<number>
    implements DocIdAsyncIterable {
    readonly canUpdateInPlace: boolean = false
    readonly size: number = 1
    readonly index: number

    constructor(index: number) {
        super()
        this.index = index
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

export function cloneIfNotReusable(
    value: BitmapAsyncIterable
): BitmapAsyncIterable {
    if (!value.canUpdateInPlace) return value.clone()
    return value
}
