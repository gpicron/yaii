import {AsyncIterableX, toArray} from "ix/asynciterable"
import {Doc, DocId, FieldName, FieldValue, ResultItem} from "../../../api/base"
import {MutableSegment} from "../../mutable-segment"
import {SortClause, SortDirection} from "../../../api/query"
import {ICompareFunction, opinionatedCompare, reverseCompareFunction} from "../../utils"
import {FieldConfigFlag} from "../../../api/config"
import * as op from "ix/asynciterable/operators"
import {BitmapDocidAsyncIterable} from "./bitmap-docid-async-iterable"
import {Heap} from "typescript-collections"

export class SortedDocidAsyncIterable extends AsyncIterableX<DocId> {
    private source: AsyncIterableX<DocId>
    private segment: MutableSegment
    private clauses: SortClause[]
    private limit: number


    constructor(source: AsyncIterableX<DocId>, segment: MutableSegment, clauses: SortClause[], limit: number = 1000) {
        super()
        this.source = source
        this.segment = segment
        this.clauses = clauses
        this.limit = limit
    }

    async* generator(): AsyncIterableIterator<DocId> {
        const comparators = new Array<ICompareFunction<ResultItem<Doc>>>()
        const sortProjection = new Array<FieldName>()

        for (const clause of this.clauses) {
            let field: FieldName
            let dir

            if (typeof clause === 'string') {
                field = clause
                dir = SortDirection.ASCENDING
            } else {
                field = clause.field
                dir = clause.dir == SortDirection.DESCENDING ? SortDirection.DESCENDING : SortDirection.ASCENDING
            }

            const config = this.segment.fieldsIndexConfig[field]

            if (!config || !(config.flags & FieldConfigFlag.STORED || config.flags & FieldConfigFlag.SORT_OPTIMIZED)) {
                throw new Error(
                    `Sorting not supported for field that is not STORED or SORT_OPTIMIZED : ${field}`
                )
            }

            let fieldComparator: ICompareFunction<ResultItem<Doc>>

            if (config.flags & FieldConfigFlag.STORED) {
                sortProjection.push(field)

                fieldComparator = (a, b) => {
                    const aElement = a[field]
                    const aVal = Array.isArray(aElement)
                        ? aElement[0]
                        : (aElement as FieldValue | undefined | Buffer)
                    const bElement = b[field]
                    const bVal = Array.isArray(bElement)
                        ? bElement[0]
                        : (bElement as FieldValue | undefined | Buffer)

                    return opinionatedCompare(aVal, bVal)
                }
            } else {
                const optimizedComparator = this.segment.getOptimizedComparator(field)

                if (optimizedComparator === undefined) throw new Error("bug")

                fieldComparator = (a, b) => {
                    return optimizedComparator(a._id, b._id)
                }
            }

            if (dir === SortDirection.DESCENDING) {
                fieldComparator = reverseCompareFunction(fieldComparator)
            }

            comparators.push(fieldComparator)
        }

        function compare(a: ResultItem<Doc>, b: ResultItem<Doc>) {
            for (const comp of comparators) {
                const v = comp(a, b);
                if (v != 0) return -v
            }
            return 0
        }

        const limit = this.limit

        let docs;

        if (sortProjection.length > 0) {
            docs = this.segment.project<Doc>(this.source, sortProjection)
        } else {
            docs = this.source.pipe(op.map(it => ({_id: it})))
        }

        let result: DocId[]

        if (BitmapDocidAsyncIterable.is(this.source) && this.source.size < limit) {
            result = (await toArray(docs)).sort(compare).map(it => it._id)
        } else {
            if (limit == 1) {
                const iter = docs[Symbol.asyncIterator]()
                let next = await iter.next()
                let r: ResultItem<Doc>;

                if (next.done) {
                    return
                } else {
                    r = next.value
                }

                for (next = await iter.next(); !next.done; next = await iter.next()) {
                    if (compare(r, next.value) > 0) {
                        r = next.value
                    }
                }

                yield r._id

                return

            } else {
                const maxHeap = new Heap<ResultItem<Doc>>(compare)

                let decount = limit

                for await (const d of docs) {
                    maxHeap.add(d)
                    if (decount > 0) {
                        decount--
                    } else {
                        maxHeap.removeRoot()
                    }
                }

                const finalSize = limit - decount
                result = new Array<DocId>(finalSize)

                for (let i = 0; i < finalSize; i++) result[i] = maxHeap.removeRoot()?._id as number
            }
        }

        for (let i = result.length - 1; i >= 0; i--) {
            yield result[i]
        }


    }


    [Symbol.asyncIterator](): AsyncIterator<DocId> {
        return this.generator();
    }

}