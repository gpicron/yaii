import {
    DEFAULT_INDEX_CONFIG,
    Doc,
    FieldConfig,
    FieldConfigFlag,
    FieldName,
    FieldsConfig,
    FieldStorableValue,
    FieldValue,
    FieldValues,
    IndexConfig,
    InvertedIndex,
    Query,
    ResultItem,
    SortClause,
    SortDirection
} from './yaii-types'
import { as, AsyncIterableX, from, toArray } from 'ix/asynciterable'
import * as op from 'ix/asynciterable/operators'

import { RoaringBitmap32 } from 'roaring'
import {
    buildExpression,
    INTERNAL_FIELDS,
    numberToTerms,
    stringToTerm,
    TERM_FALSE,
    TERM_TRUE
} from './lib/query-ir'
import { IndexSegment } from './lib/index-segment'
import {
    ExtFieldConfig,
    ExtFieldsIndexConfig,
    ExtIndexConfig,
    flattenObject,
    ICompareFunction,
    opinionatedCompare,
    reverseCompareFunction,
    Termizer
} from './lib/utils'
import { BitmapAsyncIterable, cloneIfNotReusable } from './lib/bitmap'
import { Heap } from 'typescript-collections'
import { isAsyncIterable, isIterable, isPromise } from 'ix/util/isiterable'

const NULL_TOKENIZER: Termizer = () => {
    throw new Error('Bug, should never be called')
}

interface IndexableDocWithoutSource {
    [paramKey: string]: FieldValue | FieldValues | FieldStorableValue
}

export type IndexableDoc = IndexableDocWithoutSource & {
    '£_SOURCE'?: Doc
}

type DocPipelineMapFunction = (input: Doc) => Promise<Doc> | Doc
type IndexableDocPipelineMapFunction = (
    input: IndexableDoc
) => Promise<IndexableDoc> | IndexableDoc

export class MemoryInvertedIndex implements InvertedIndex {
    readonly fieldsConfig: ExtFieldsIndexConfig
    private segment: IndexSegment
    private deleted = new RoaringBitmap32()
    private indexConfig: ExtIndexConfig
    private fieldGenerators = new Array<DocPipelineMapFunction>()
    private allFieldGenerator?: IndexableDocPipelineMapFunction

    constructor(
        fieldsConfig: FieldsConfig = {},
        config: Partial<IndexConfig> = DEFAULT_INDEX_CONFIG
    ) {
        const fields: ExtFieldsIndexConfig = {}

        const indexConfig = Object.assign(
            Object.assign({}, DEFAULT_INDEX_CONFIG),
            config
        )

        for (const [field, fConf] of Object.entries(fieldsConfig)) {
            fields[field] = this.extractFieldConfig(fConf)
            if (fConf.generator) {
                const valueGenerator = fConf.generator
                this.fieldGenerators.push(async (input: Doc) => {
                    input[field] = await toArray(as(valueGenerator(input)))

                    return input
                })
            }
        }

        this.fieldsConfig = fields
        this.indexConfig = {
            defaultFieldConfig: this.extractFieldConfig(
                indexConfig.defaultFieldConfig
            ),
            storeSourceDoc:
                indexConfig.storeSourceDoc == undefined
                    ? true
                    : indexConfig.storeSourceDoc,
            allFieldConfig: this.extractFieldConfig(indexConfig.allFieldConfig)
        }
        // only searchable is allowed
        this.indexConfig.allFieldConfig.flags =
            this.indexConfig.allFieldConfig.flags & FieldConfigFlag.SEARCHABLE

        if (this.indexConfig.storeSourceDoc) {
            fields[INTERNAL_FIELDS.SOURCE] = {
                all: false,
                flags: 0,
                tokenizer: NULL_TOKENIZER
            }
        }

        if (
            this.indexConfig.allFieldConfig &&
            this.indexConfig.allFieldConfig.flags & FieldConfigFlag.SEARCHABLE
        ) {
            fields[INTERNAL_FIELDS.ALL] = this.indexConfig.allFieldConfig

            const defaultToAll = this.indexConfig.defaultFieldConfig.all

            this.allFieldGenerator = function(input: IndexableDoc) {
                const allValues = new Array<FieldValue>()

                for (const [field, value] of Object.entries(input)) {
                    if (value && !Buffer.isBuffer(value)) {
                        const fc = fields[field]
                        if ((fc && fc.all) || (!fc && defaultToAll)) {
                            if (Array.isArray(value)) {
                                value.forEach(v => allValues.push(v))
                            } else {
                                allValues.push(value as FieldValue)
                            }
                        }
                    }
                }

                input[INTERNAL_FIELDS.ALL] = allValues

                return input
            }
        }

        this.segment = new IndexSegment(fields, this.indexConfig)
    }

    private valueTermizer(val: FieldValue) {
        if (typeof val === 'string') {
            return [stringToTerm(val)]
        } else if (typeof val === 'boolean') {
            return val ? [TERM_TRUE] : [TERM_FALSE]
        } else {
            return numberToTerms(val)
        }
    }

    private extractFieldConfig(fConf: FieldConfig): ExtFieldConfig {
        let termizer: Termizer
        if (fConf.flags & FieldConfigFlag.SEARCHABLE) {
            const analyzer = fConf.analyzer
            if (analyzer) {
                termizer = input => {
                    if (Array.isArray(input)) {
                        const analyzerResult: FieldValue[] = new Array<
                            FieldValue
                        >().concat(...input.map(analyzer))
                        const termizerResult: Buffer[][] = analyzerResult.map(
                            this.valueTermizer
                        )
                        return new Array<Buffer>().concat(...termizerResult)
                    } else {
                        const termizerResult: Buffer[][] = analyzer(input).map(
                            this.valueTermizer
                        )
                        return new Array<Buffer>().concat(...termizerResult)
                    }
                }
            } else {
                termizer = input => {
                    if (Array.isArray(input)) {
                        const termizerResult: Buffer[][] = input.map(
                            this.valueTermizer
                        )
                        return new Array<Buffer>().concat(...termizerResult)
                    } else {
                        return this.valueTermizer(input)
                    }
                }
            }
        } else {
            termizer = NULL_TOKENIZER
        }

        return {
            flags: fConf.flags,
            all: fConf.addToAllField == undefined ? true : fConf.addToAllField,
            tokenizer: termizer
        }
    }

    async add(
        doc: Doc | Promise<Doc> | AsyncIterable<Doc> | Iterable<Doc>
    ): Promise<number> {
        let input: AsyncIterableX<Doc>

        if (isAsyncIterable(doc) || isIterable(doc)) {
            input = as(doc)
        } else if (isPromise(doc)) {
            input = as([doc])
        } else {
            input = as([doc])
        }

        for (const m of this.fieldGenerators) {
            input = input.pipe(op.map(m, this))
        }

        let indexables: AsyncIterableX<IndexableDoc>

        if (this.indexConfig.storeSourceDoc) {
            indexables = input.pipe(
                op.map((source: Doc) => {
                    const indexableDoc: IndexableDoc = flattenObject(source)
                    indexableDoc[INTERNAL_FIELDS.SOURCE] = source

                    return indexableDoc
                })
            )
        } else {
            indexables = input.pipe(op.map(flattenObject))
        }

        if (this.allFieldGenerator) {
            indexables = indexables.pipe(op.map(this.allFieldGenerator))
        }

        return this.segment.add(indexables)
    }

    query(
        query: Query,
        projection?: Array<FieldName>,
        limit: number = 1000,
        sort?: Array<SortClause>
    ): AsyncIterableX<ResultItem> {
        const actualProjection =
            projection == undefined
                ? this.indexConfig.storeSourceDoc
                    ? [INTERNAL_FIELDS.SOURCE]
                    : Object.entries(this.fieldsConfig)
                          .filter(e => e[1].flags & FieldConfigFlag.STORED)
                          .map(e => e[0])
                : projection

        const segment = this.segment
        const deleted = this.deleted

        let sortComparator: ICompareFunction<ResultItem> | undefined
        let sortProjection: Array<FieldName> | undefined

        if (sort) {
            sortProjection = new Array<FieldName>()
            for (const clause of sort) {
                let field: FieldName
                let dir: SortDirection
                if (typeof clause === 'string') {
                    field = clause
                    dir = SortDirection.ASCENDING
                } else {
                    field = clause.field
                    dir = clause.dir || SortDirection.ASCENDING
                }

                const config = this.fieldsConfig[field]

                if (
                    !config ||
                    !(
                        config.flags & FieldConfigFlag.STORED ||
                        !this.indexConfig.storeSourceDoc
                    )
                )
                    throw new Error(
                        `Sorting not supported for field that is not stored: ${field}`
                    )

                sortProjection.push(field)

                let fieldComparator: ICompareFunction<ResultItem> = (a, b) => {
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

                if (dir == SortDirection.DESCENDING) {
                    fieldComparator = reverseCompareFunction(fieldComparator)
                }

                if (sortComparator) {
                    const firstCompare = sortComparator

                    sortComparator = (a, b) => {
                        const r = firstCompare(a, b)
                        if (r == 0) {
                            return fieldComparator(a, b)
                        } else {
                            return r
                        }
                    }
                } else {
                    sortComparator = (a, b) => {
                        return fieldComparator(a, b)
                    }
                }
            }
        }

        const resolveAndProjectSegment = async function*() {
            let exp = await buildExpression(query)

            const segmentLast = segment.next

            exp = exp.rewrite(segment)

            let docIds: AsyncIterableX<number> = await exp.resolve(segment)

            if (BitmapAsyncIterable.is(docIds)) {
                docIds.removeRange(segmentLast)
                docIds = cloneIfNotReusable(docIds).andNotInPlace(
                    new BitmapAsyncIterable(deleted, false)
                )
            } else {
                docIds = as(docIds).pipe(
                    op.filter(e => e < segmentLast && !deleted.has(e))
                )
            }

            if (sortComparator && sortProjection) {
                const heap = new Heap<ResultItem>(
                    reverseCompareFunction(sortComparator)
                )

                for await (const d of segment.project(docIds, sortProjection)) {
                    heap.add(d)
                    if (heap.size() > limit) {
                        heap.removeRoot()
                    }
                }

                const result = new Array<number>()

                let d = heap.removeRoot()
                while (d) {
                    result.unshift(d._id)
                    d = heap.removeRoot()
                }

                docIds = as(result)
            }

            let decount = limit

            for await (const doc of segment.project(docIds, actualProjection)) {
                yield doc
                if (--decount == 0) return
            }
        }

        return from(resolveAndProjectSegment())
    }

    get size() {
        return this.segment.size - this.deleted.size
    }

    listAllKnownField(): Record<string, FieldConfig> {
        return this.fieldsConfig
    }
}

/// ------------------------------------------------------
