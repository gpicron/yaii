import {BitmapDocidAsyncIterable} from "../datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {AggregateResults} from "../../api/base"
import {AsyncIterableX} from "ix/asynciterable"
import {BaseSegment} from "../segments/segment"
import {DocIdIterable} from "../datastructs/docid-async-iterable/base"
import {RangeDocidAsyncIterable} from "../datastructs/docid-async-iterable/range-docid-async-iterable"

export abstract class Exp {
    // eslint-disable-next-line
    rewrite(segment: BaseSegment): Exp {
        return this
    }

    abstract async resolve(segment: BaseSegment, forceResolveToBitmap?: boolean): Promise<DocIdIterable>
}

export class AllExp extends Exp {
    async resolve(segment: BaseSegment): Promise<DocIdIterable> {
        return new RangeDocidAsyncIterable(0, segment.rangeSize)
    }

    toString(): string {
        return 'ALL'
    }
}

export const ALL_EXP = new AllExp()

export class NoneExp extends Exp {
    async resolve(): Promise<DocIdIterable> {
        return new BitmapDocidAsyncIterable()
    }

    toString(): string {
        return 'NONE'
    }
}


export type AggregateProcessor = (docIds: AsyncIterableX<number>) => Promise<AggregateResults>
