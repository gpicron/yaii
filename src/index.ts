import {DEFAULT_INDEX_CONFIG, InvertedIndex} from './yaii-types'
import {Doc, DocId, FieldName, FieldStorableValue, FieldValue, FieldValues, ResultItem} from './lib/api/base'
import {Query, SortClause} from './lib/api/query'
import {FieldConfig, FieldConfigFlag, FieldsConfig, IndexConfig, ValueGenerator} from "./lib/api/config"

import {buildExpression, numberToTerms, stringToTerm, TERM_FALSE, TERM_TRUE} from './lib/internal/query-ir/query-ir'
import {MutableSegment} from './lib/internal/mutable-segment'
import {
    ExtFieldConfig,
    ExtFieldsIndexConfig,
    ExtIndexConfig,
    flattenObject,
    INTERNAL_FIELDS,
    Termizer
} from './lib/internal/utils'
import {
    cloneIfNotReusable,
    SingletonDocidAsyncIterable
} from './lib/internal/datastructs/docid-async-iterable/singleton-docid-async-iterable'
import {BitmapDocidAsyncIterable} from "./lib/internal/datastructs/docid-async-iterable/bitmap-docid-async-iterable"

import {as, AsyncIterableX, from} from 'ix/asynciterable'
import * as op from 'ix/asynciterable/operators'
import {isAsyncIterable, isIterable, isPromise} from 'ix/util/isiterable'
import * as util from "util"
import {Term} from "./lib/internal/query-ir/term-exp"
import {Aggregation} from "./lib/api/aggregation"
import {SortedDocidAsyncIterable} from "./lib/internal/datastructs/docid-async-iterable/sorted-docid-async-iterable"


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
    private segment: MutableSegment
    private deleted = new BitmapDocidAsyncIterable()
    private indexConfig: ExtIndexConfig
    private fieldGenerators = new Array<{field: FieldName, valueGenerator: ValueGenerator}>()
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
                this.fieldGenerators.push({field: field, valueGenerator: fConf.generator})
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
                        if (fc && fc.all || !fc && defaultToAll) {
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

        this.segment = new MutableSegment(fields, this.indexConfig)
    }

    private valueTermizer(val: FieldValue): Term[] {
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
                        const analyzerResult: FieldValue[] = new Array<FieldValue>().concat(...input.map(analyzer))
                        const termizerResult: Term[][] = analyzerResult.map(this.valueTermizer)
                        return new Array<Term>().concat(...termizerResult)
                    } else {
                        const termizerResult: Term[][] = analyzer(input).map(
                            this.valueTermizer
                        )
                        return new Array<Term>().concat(...termizerResult)
                    }
                }
            } else {
                termizer = input => {
                    if (Array.isArray(input)) {
                        const termizerResult: Term[][] = input.map(
                            this.valueTermizer
                        )
                        return new Array<Term>().concat(...termizerResult)
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

        let indexables: AsyncIterableX<IndexableDoc>

        if (this.indexConfig.storeSourceDoc) {
            indexables = input.pipe(
                op.map((source: Doc) => {
                    try {
                        const indexableDoc: IndexableDoc = flattenObject(source)

                        for (const generator of this.fieldGenerators) {
                            indexableDoc[generator.field] = generator.valueGenerator(source)
                        }

                        indexableDoc[INTERNAL_FIELDS.SOURCE] = source

                        return indexableDoc
                    } catch (e) {
                        console.error("failed to convert doc to indexable doc", util.inspect(source, false, null, true))
                        return {}
                    }
                })
            )
        } else {
            indexables = input.pipe(op.map((source: Doc) => {
                try {
                    const indexableDoc: IndexableDoc = flattenObject(source)

                    for (const generator of this.fieldGenerators) {
                        indexableDoc[generator.field] = generator.valueGenerator(source)
                    }

                    return indexableDoc
                } catch (e) {
                    console.error("failed to convert doc to indexable doc", util.inspect(source, false, null, true))
                    return {}
                }
            }))
        }

        if (this.allFieldGenerator) {
            indexables = indexables.pipe(op.map(this.allFieldGenerator))
        }

        return this.segment.add(indexables)
    }


    aggregateQuery(
            filter: Query,
            aggregations: Array<Aggregation>,
            sort?: Array<SortClause>) {
        console.log(filter)
        console.log(aggregations)
        console.log(sort)

    }


    query<T extends Doc>(
        filter: Query,
        sort?: Array<SortClause>,
        limit?: number,
        projection?: Array<FieldName>
    ): AsyncIterableX<ResultItem<T>> {
        let actualProjection: Array<FieldName> ;

        if (projection == undefined) {
            if (this.indexConfig.storeSourceDoc) {
                actualProjection = [INTERNAL_FIELDS.SOURCE]
            } else {
                actualProjection = Object.entries(this.fieldsConfig)
                    .filter(e => e[1].flags & FieldConfigFlag.STORED)
                    .map(e => e[0])
            }
        } else {
            actualProjection = projection
        }
        
        const segment = this.segment
        const deleted = this.deleted

        const resolveAndProjectSegment = async function*() {
            let exp = buildExpression(filter, segment)

            const segmentLast = segment.next

            exp = exp.rewrite(segment)

            let docIds: AsyncIterableX<DocId> = await exp.resolve(segment)

            if (BitmapDocidAsyncIterable.is(docIds)) {
                docIds = cloneIfNotReusable(docIds).removeRange(segmentLast).andNotInPlace(deleted)
            } else if (SingletonDocidAsyncIterable.is(docIds)) {
                docIds = deleted.has(docIds.index) ? BitmapDocidAsyncIterable.EMPTY_MAP : docIds
            } else {
                docIds = as(docIds).pipe(
                    op.filter(e => e < segmentLast && !deleted.has(e))
                )
            }

            if (Array.isArray(sort) && sort.length > 0) {
                if (!SingletonDocidAsyncIterable.is(docIds) ||
                    !(BitmapDocidAsyncIterable.is(docIds) && docIds.hasMoreThanOne())) {

                    limit = limit || 1000
                    docIds = new SortedDocidAsyncIterable(docIds, segment, sort, limit)
                }
            }

            let decount = limit || Number.MAX_SAFE_INTEGER

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

