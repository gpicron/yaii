import {DocId} from "../../../api/base"
import {DocIdIterable, DocIdIterator, NO_MORE_DOC} from "./base"
import {RoaringBitmap32} from "roaring"

export class BitmapDocidAsyncIterable extends DocIdIterable {
    readonly bitmap: RoaringBitmap32
    readonly mutable: boolean

    private _readonly: BitmapDocidAsyncIterable

    constructor(mutable: boolean = true, bitmap?: RoaringBitmap32) {
        super()
        this.bitmap = bitmap || new RoaringBitmap32()
        this.mutable = mutable
        if (mutable) {
            this._readonly = new BitmapDocidAsyncIterable(false, this.bitmap)
        } else {
            this._readonly = this
        }
    }

    static is(x: DocIdIterable): x is BitmapDocidAsyncIterable {
        return x instanceof BitmapDocidAsyncIterable
    }

    get cost(): number {
        return this.bitmap.size
    }

    [Symbol.iterator](): DocIdIterator {
        const current = {
            value: -1,
            done: false
        }

        const map = this.bitmap

        return {
            next(skipUntil?: DocId): IteratorResult<DocId, unknown> {
                if (skipUntil && skipUntil < current.value) throw new Error("skipUntil lower than current")
                const goto = skipUntil || current.value + 1
                if (map.has(goto)) {
                    current.value = goto
                    return current
                } else {
                    const next = map.select(map.rank(goto))
                    if (next) {
                        current.value = next
                        return current
                    } else {
                        return NO_MORE_DOC
                    }
                }
            }
        };
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


    flipRange(from: number, to: number): this {
        if (!this.mutable) throw new Error('Immutable')

        this.bitmap.flipRange(from, to)
        return this
    }


    has(e: DocId): boolean {
        return this.bitmap.has(e);
    }

    hasMoreThanOne() {
        return this.bitmap.select(1) !== undefined;
    }


    get sizeInMemory() {
        return this.bitmap.getSerializationSizeInBytes()
    }

    readOnly() {
        return this._readonly;
    }
}
