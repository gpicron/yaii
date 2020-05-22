import {AsyncIterableX} from "ix/asynciterable"
import {ICompareFunction} from "./utils"
import {UnaryFunction} from "ix/interfaces"
import Heap from "./datastructs/binary-heap"

class TopByOrderedAsyncIterable<TSource, TKey> extends AsyncIterableX<TSource> {
    readonly source: AsyncIterable<TSource>
    readonly limit: number
    readonly comparator: ICompareFunction<TKey>
    readonly keySelector: UnaryFunction<TSource, TKey>

    constructor(source: AsyncIterable<TSource>, limit: number, keySelector: UnaryFunction<TSource, TKey>, comparator: ICompareFunction<TKey>) {
        super()
        this.source = source
        this.limit = limit
        this.comparator = comparator
        this.keySelector = keySelector
    }


    async* [Symbol.asyncIterator](): AsyncGenerator<TSource, void, unknown> {
        if (this.limit == 1) {
            const iter = this.source[Symbol.asyncIterator]()
            let next = await iter.next()
            let r: TSource;
            let rK: TKey;

            if (next.done) {
                return
            } else {
                r = next.value
                rK = this.keySelector(r)
            }

            for (next = await iter.next(); !next.done; next = await iter.next()) {
                const nextK = this.keySelector(next.value)

                if (this.comparator(rK, nextK) > 0) {
                    r = next.value
                    rK = nextK
                }
            }

            yield r

            return

        } else {
            const maxHeap = new Heap<TSource>((a: TSource, b: TSource) => this.comparator(this.keySelector(a), this.keySelector(b)))

            let decount = this.limit

            for await (const d of this.source) {
                maxHeap.add(d)
                if (decount > 0) {
                    decount--
                } else {
                    maxHeap.removeRoot()
                }
            }

            const finalSize = this.limit - decount
            const result = new Array<TSource>(finalSize)

            for (let i = 0; i < finalSize; i++) result[i] = maxHeap.removeRoot() as TSource
            for (let i = result.length - 1; i >= 0; i--) {
                yield result[i]
            }
        }
    }
}

export function topBy<TSource, TKey>(limit: number, keySelector: (item: TSource) => TKey, comparator: ICompareFunction<TKey>): UnaryFunction<AsyncIterable<TSource>, TopByOrderedAsyncIterable<TSource, TKey>> {
    return (source) => new TopByOrderedAsyncIterable<TSource, TKey>(source, limit, keySelector, comparator);
}
