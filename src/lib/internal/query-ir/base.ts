import {MutableSegment} from "../mutable-segment"
import {DocidAsyncIterable} from "../datastructs/docid-async-iterable/docid-async-iterable"
import {RoaringBitmap32} from "roaring"
import {BitmapDocidAsyncIterable} from "../datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {AggregateResults} from "../../api/base"
import {AsyncIterableX} from "ix/asynciterable"

export abstract class Exp {
    // eslint-disable-next-line
    rewrite(segment: MutableSegment): Exp {
        return this
    }

    abstract async resolve(segment: MutableSegment, forceResolveToBitmap?: boolean): Promise<DocidAsyncIterable>
}

export class AllExp extends Exp {
    async resolve(segment: MutableSegment): Promise<DocidAsyncIterable> {
        const m = RoaringBitmap32.fromRange(segment.from, segment.size + segment.from)
        return new BitmapDocidAsyncIterable(true, m)
    }

    toString(): string {
        return 'ALL'
    }
}

export const ALL_EXP = new AllExp()

export class NoneExp extends Exp {
    async resolve(): Promise<DocidAsyncIterable> {
        return new BitmapDocidAsyncIterable()
    }

    toString(): string {
        return 'NONE'
    }
}


export type AggregateProcessor = (docIds: AsyncIterableX<number>) => Promise<AggregateResults>
