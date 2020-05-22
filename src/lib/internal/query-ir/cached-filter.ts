import {Exp} from "./base"
import {Query, TokenQuery} from "../../api/query"
import {INTERNAL_FIELDS} from "../utils"
import {QueryOperator} from "../../api/base"
import {BaseSegment} from "../segments/segment"
import {buildFilterExpression} from "./query-ir"
import {BitmapDocidAsyncIterable} from "../datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {DocIdIterable} from "../datastructs/docid-async-iterable/base"

export class CachedFilter implements Exp, TokenQuery {
    readonly field = INTERNAL_FIELDS.FILTER_CACHE
    readonly operator = QueryOperator.TOKEN
    readonly value: string
    readonly query: Query
    private cache = new Map<number, DocIdIterable>()

    constructor(name: string, query: Query) {
        this.value = name
        this.query = query
    }

    async resolve(segment: BaseSegment): Promise<DocIdIterable> {
        let docids = this.cache.get(segment.id)
        if (!docids) {
            const exp = buildFilterExpression(this.query, segment)
            docids = await exp.rewrite(segment).resolve(segment, true)
            if (BitmapDocidAsyncIterable.is(docids)) {
                docids = new BitmapDocidAsyncIterable(false, docids.bitmap)
            }

            this.cache.set(segment.id, docids)
        }

        return docids
    }

    rewrite(): Exp {
        return this;
    }

}
