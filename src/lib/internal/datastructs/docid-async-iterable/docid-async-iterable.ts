
import {BitmapDocidAsyncIterable} from "./bitmap-docid-async-iterable"
import {DocId} from "../../../api/base"
import {SingletonDocidAsyncIterable} from "./singleton-docid-async-iterable"

export declare abstract class DocidAsyncIterable implements AsyncIterable<DocId> {
    readonly mutable: boolean
    readonly size: number

    clone(): BitmapDocidAsyncIterable

    add(index: number): this

    remove(index: number): this

    removeRange(low: number, high: number): this

    andInPlace(and: DocidAsyncIterable): this

    andNotInPlace(andNot: DocidAsyncIterable): this

    flipRange(from: number, to: number): this

    has(e: DocId): boolean

    [Symbol.asyncIterator](): AsyncIterator<number>


}

export function orMany(opers: DocidAsyncIterable[]): BitmapDocidAsyncIterable {
    let result: BitmapDocidAsyncIterable | undefined
    const maps = opers.filter(BitmapDocidAsyncIterable.is)
    if (maps.length > 0) {
        result = BitmapDocidAsyncIterable.orManyBitmap(maps)
    }
    if (maps.length < opers.length) {
        if (!result) result = new BitmapDocidAsyncIterable()

        for (const s of opers) {
            // eslint-disable-next-line
            if (SingletonDocidAsyncIterable.is(s)) {
                result.add(s.index)
            }
        }
    }

    return result as BitmapDocidAsyncIterable
}
