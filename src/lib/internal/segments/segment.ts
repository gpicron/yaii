import {ExtFieldsIndexConfig, ExtIndexConfig} from "../utils"
import {Doc, DocId, FieldName, ResultItem} from "../../api/base"
import {as, AsyncIterableX, first} from "ix/asynciterable"
import {Term, TermExp} from "../query-ir/term-exp"
import {BitmapDocidAsyncIterable} from "../datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {ProjectionsAndComparator} from "../../../base-inverted-index"
import {SortClause} from "../../api/query"
import * as op from "ix/asynciterable/operators"
import {DocIdIterable} from "../datastructs/docid-async-iterable/base"
import {SingletonDocidAsyncIterable} from "../datastructs/docid-async-iterable/singleton-docid-async-iterable"

export abstract class BaseSegment {
    readonly id: number
    readonly fieldsIndexConfig: ExtFieldsIndexConfig
    readonly indexConfig: ExtIndexConfig

    readonly from: number
    abstract readonly rangeSize: number
    abstract readonly next: number


    constructor(id: number, fieldsIndexConfig: ExtFieldsIndexConfig, indexConfig: ExtIndexConfig, from: number) {
        this.id = id
        this.fieldsIndexConfig = fieldsIndexConfig
        this.indexConfig = indexConfig
        this.from = from
    }

    project<T extends Doc>(docIds: DocIdIterable, projection?: Array<FieldName>): AsyncIterableX<ResultItem<T>> {
        const results = as(docIds).pipe(op.map(id => ({
            _id: id
        })))
        return this.addProjections(results, projection)
    }

    async projectDoc<T extends Doc>(docId: DocId, projection?: FieldName[]): Promise<ResultItem<T>> {
        const result = await first(this.project<T>(new SingletonDocidAsyncIterable(docId), projection))
        return result || {
            _id: docId
        }
    }


    abstract get(field: FieldName, term: Term): DocIdIterable | Promise<DocIdIterable>

    abstract mayMatch(term: TermExp): boolean | Promise<boolean>

    abstract terms(field: string): AsyncIterableX<Term>

    abstract deleted(): BitmapDocidAsyncIterable

    abstract addToDeleted(docId: DocId): void

    abstract buildComparatorAndProjections(sort: Array<SortClause>): ProjectionsAndComparator

    abstract addProjections<T extends Doc>(source: AsyncIterable<ResultItem<T>>, projection?: string[]): AsyncIterableX<ResultItem<T>>
}
