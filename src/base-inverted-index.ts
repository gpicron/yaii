import {
    AggregateResult,
    AggregateResults,
    DEFAULT_INDEX_CONFIG,
    Doc,
    DocId,
    FieldConfig,
    FieldConfigFlag,
    FieldName,
    FieldsConfig,
    FieldValue,
    IndexConfig,
    InvertedIndex,
    Query,
    QueryMode,
    ResultItem,
    SortClause,
    SortDirection,
    ValueGenerator
} from "./yaii-types"
import {
    assertUnreachable,
    ExtFieldConfig,
    ExtFieldsIndexConfig,
    ExtIndexConfig,
    flattenObject,
    ICompareFunction,
    INTERNAL_FIELDS,
    opinionatedCompare,
    removeDeletedAndAddedAfter,
    reverseCompareFunction,
    SegmentRange,
    Termizer,
    waitForImmediate
} from "./lib/internal/utils"
import {MutableSegment} from "./lib/internal/segments/mutable-segment"
import {Term} from "./lib/internal/query-ir/term-exp"
import {
    buildFilterExpression,
    numberToTerms,
    stringToTerm,
    TERM_FALSE,
    TERM_TRUE
} from "./lib/internal/query-ir/query-ir"
import {as, AsyncIterableX, concat, from, fromEvent, sum} from "ix/asynciterable"
import {isAsyncIterable, isIterable, isPromise} from "ix/util/isiterable"
import * as op from "ix/asynciterable/operators"
import * as ops from "ix/iterable/operators"
import * as util from "util"
import {
    Aggregation,
    isCountDocAggregation,
    isFirstAggregation,
    isGroupByAggregation,
    isLastAggregation
} from "./lib/api/aggregation"
import {BaseSegment} from "./lib/internal/segments/segment"
import {IndexableDoc, IndexableDocPipelineMapFunction, NULL_TOKENIZER} from "./index"
import {RWLock} from "./lib/internal/mutex/rwlock"
import {ImmutableSegment} from "./lib/internal/segments/immutable-segment"
import fs from "fs"
import {
    AggrAccumulator,
    AggrAccumulatorMerger,
    CountAccumulator,
    CountAccumulatorMerger,
    GroupByAccumulator,
    GroupByAccumulatorMerger,
    TopAccumulator,
    TopAccumulatorMerger
} from "./lib/internal/aggregate"
import {topBy} from "./lib/internal/topby-async-iterable"
import {EventEmitter} from "events"
import {removeAll} from "./lib/internal/arrays"


interface AddedEvent {
    readonly segment: MutableSegment
    readonly from: DocId
    readonly count: number
}

export class BaseInvertedIndex implements InvertedIndex {
    readonly fieldsConfig: ExtFieldsIndexConfig
    private otherSegments = new Array<BaseSegment>()
    private currentSegment: MutableSegment
    private indexConfig: ExtIndexConfig
    private fieldGenerators = new Array<{ field: FieldName, valueGenerator: ValueGenerator }>()
    private allFieldGenerator?: IndexableDocPipelineMapFunction

    private rwLock = new RWLock()
    private nextSegmentId = 0;

    private updateEventEmitter = new EventEmitter()

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
            defaultFieldConfig: this.extractFieldConfig(indexConfig.defaultFieldConfig),
            storeSourceDoc: indexConfig.storeSourceDoc == undefined ? true : indexConfig.storeSourceDoc,
            allFieldConfig: this.extractFieldConfig(indexConfig.allFieldConfig),
            storePath: indexConfig.storePath || './db'
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

            this.allFieldGenerator = function (input: IndexableDoc) {
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

        fs.mkdirSync(this.indexConfig.storePath, {
            recursive: true
        })

        this.currentSegment = new MutableSegment(++this.nextSegmentId, fields, this.indexConfig, 0)
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

        return sum(indexables.pipe(
            op.buffer(1000),
            op.map(async buffer => {
                // read the current segment to push data.  And want to prevent the stating of the current segment
                // while doing so
                return this.rwLock.dispatchRead(() => {
                    const nextDocId = this.currentSegment.next
                    const count = this.currentSegment.add(buffer)
                    this.updateEventEmitter.emit("added", {
                        segment: this.currentSegment,
                        from: nextDocId,
                        count: count
                    } as AddedEvent)
                    return count
                })
            })
        ))

    }



    async aggregateQuery(
        filter: Query,
        aggregations: Array<Aggregation>): Promise<AggregateResults> {

        const accumulatorMergers = this.prepareAccumulatorMergers(aggregations)

        for await (const range of this.currentSegments()) {

            let exp = buildFilterExpression(filter, range.segment)

            exp = exp.rewrite(range.segment)

            let docIds = await exp.resolve(range.segment)

            docIds = removeDeletedAndAddedAfter(docIds, range)

            const segmentAccAndProjections = this.prepareAccumulators(aggregations, range)

            let docs;
            if (segmentAccAndProjections.requiredProjections.length > 0) {
                docs = range.segment.project<Doc>(docIds, segmentAccAndProjections.requiredProjections)
            } else {
                docs = docIds.pipe(ops.map(it => ({_id: it})))
            }

            let count = 0
            for await (const d of docs) {
                for (let i = 0; i < aggregations.length; i++) {
                    segmentAccAndProjections.accumulators[i].accumulate(d)
                }
                count++
            }

            for (let i = 0; i < aggregations.length; i++) {
                accumulatorMergers[i].accumulate( await segmentAccAndProjections.accumulators[i].finalize(count))
            }

        }

        return Promise.all(accumulatorMergers.map(async m => m.finalize() as Promise<AggregateResult<unknown>>))
    }

    private prepareAccumulatorMergers(aggregations: Array<Aggregation>) {

        const mergers = new Array<AggrAccumulatorMerger<Aggregation, unknown, AggregateResult<unknown>>>()

        for (let i = 0; i < aggregations.length; i++) {
            const agg = aggregations[i]
            if (isCountDocAggregation(agg)) {
                mergers.push(new CountAccumulatorMerger(agg))
            } else if (isFirstAggregation(agg)) {
                mergers.push(new TopAccumulatorMerger(agg, true))
            } else if (isLastAggregation(agg)) {
                mergers.push(new TopAccumulatorMerger(agg, false))
            } else if (isGroupByAggregation(agg)) {
                const prepared = () => this.prepareAccumulatorMergers(agg.aggregations)

                mergers.push(new GroupByAccumulatorMerger(agg, prepared))
            }
        }
        return mergers
    }

    private prepareAccumulators(aggregations: Array<Aggregation>, range: SegmentRange) {
        const requiredProjections = new Set<string>()

        const accumulators = new Array<AggrAccumulator<Aggregation, AggregateResult<unknown>>>()

        for (let i = 0; i < aggregations.length; i++) {
            const agg = aggregations[i]
            if (isCountDocAggregation(agg)) {
                accumulators.push(new CountAccumulator(agg))
            } else if (isFirstAggregation(agg)) {
                const cAndP = range.segment.buildComparatorAndProjections(agg.sort)

                cAndP.projections.forEach(it => requiredProjections.add(it))

                accumulators.push(new TopAccumulator(reverseCompareFunction(cAndP.comparator), range, agg))
            } else if (isLastAggregation(agg)) {
                const cAndP = range.segment.buildComparatorAndProjections(agg.sort)

                cAndP.projections.forEach(it => requiredProjections.add(it))

                accumulators.push(new TopAccumulator(cAndP.comparator, range, agg))
            } else if (isGroupByAggregation(agg)) {
                const prepared = () => this.prepareAccumulators(agg.aggregations, range).accumulators

                requiredProjections.add(agg.fieldName)
                this.prepareAccumulators(agg.aggregations, range).requiredProjections.forEach((it: string) => requiredProjections.add(it))

                accumulators.push(new GroupByAccumulator(agg, range, prepared))
            }
        }
        return {
            requiredProjections: Array.from(requiredProjections),
            accumulators: accumulators
        }
    }

    private static async resolveAndProject<T extends Doc>(segmentRange: SegmentRange, filter: Query, projections: FieldName[] | undefined): Promise<AsyncIterableX<ResultItem<T>>> {
        const segment = segmentRange.segment
        let exp = buildFilterExpression(filter, segment)

        exp = exp.rewrite(segment)

        let docIds = await exp.resolve(segment)
        docIds = removeDeletedAndAddedAfter(docIds, segmentRange)

        return segment.project<T>(docIds, projections)
    }

    query<T extends Doc>(
        filter: Query,
        sort?: Array<SortClause>,
        limit?: number,
        projection?: Array<FieldName>,
        mode: QueryMode = QueryMode.CURRENT
    ): AsyncIterableX<ResultItem<T>> {
        let actualProjection: Array<FieldName> | undefined;

        if (projection == undefined) {
            if (!this.indexConfig.storeSourceDoc) {
                actualProjection = Object.entries(this.fieldsConfig)
                    .filter(e => e[1].flags & FieldConfigFlag.STORED)
                    .map(e => e[0])
            }
        } else {
            actualProjection = projection
        }

        let result: AsyncIterableX<ResultItem<T>>


        if (sort) {
            if (mode === QueryMode.FUTURE) throw new Error("future queries cannot have sort clauses")

            const actualLimit = limit || 1000

            const generalComparatorAndProjection = buildComparatorAndProjections(sort)

            result = from(this.currentSegments()).pipe(
                op.flatMap(async (range: SegmentRange) => {
                        const segmentComparatorAndProjection = range.segment.buildComparatorAndProjections(sort)

                        const topNOfSegment = (await BaseInvertedIndex.resolveAndProject<T>(range, filter, segmentComparatorAndProjection.projections))
                            .pipe(topBy(actualLimit, item => item, segmentComparatorAndProjection.comparator))

                        const missingGeneralProjections = removeAll(generalComparatorAndProjection.projections, segmentComparatorAndProjection.projections)

                        return range.segment.addProjections(topNOfSegment, missingGeneralProjections).pipe(
                            op.map(value => ({
                                _segment: range.segment,
                                value: value
                            }))
                        )
                }),
                topBy(actualLimit, item => item.value, generalComparatorAndProjection.comparator),
                op.map(async item => item._segment.projectDoc<T>(item.value._id, actualProjection))
            )

            if (mode === QueryMode.CURRENT_AND_FUTURE) {
                result = result.pipe(op.take(actualLimit))
                limit = undefined

                const future = fromEvent<AddedEvent>(this.updateEventEmitter, "added").pipe(
                    op.flatMap(async (segmentRange: SegmentRange) =>
                        BaseInvertedIndex.resolveAndProject<T>(segmentRange, filter, actualProjection))
                )

                result = concat(result,future)
            }

        } else {
            let segments = from(this.currentSegments())

            switch (mode) {
                case QueryMode.CURRENT:
                    segments = from(this.currentSegments())
                    break;
                case QueryMode.CURRENT_AND_FUTURE:
                    segments = concat(from(this.currentSegments()), fromEvent<AddedEvent>(this.updateEventEmitter, "added"))
                    break;
                case QueryMode.FUTURE:
                    segments = fromEvent<AddedEvent>(this.updateEventEmitter, "added")
                    break;
                default:
                    return assertUnreachable(mode)
            }

            result = segments.pipe(
                op.flatMap(async (segmentRange: SegmentRange) =>
                    BaseInvertedIndex.resolveAndProject<T>(segmentRange, filter, actualProjection))
            )
        }

        if (limit && limit > 0 && Number.isInteger(limit)) {
            result = result.pipe(op.take(limit))
        }

        return result
    }

    async size(): Promise<number> {
        let size = 0;
        for await (const s of this.currentSegments()) {
            size += s.count - s.segment.deleted().cost
        }
        return size
    }

    listAllKnownField(): Record<string, FieldConfig> {
        return this.fieldsConfig
    }

    async commit(sync: boolean = false): Promise<void> {
        if (this.currentSegment.rangeSize > 0) {
            if (sync) {
                return this.rwLock.dispatchWrite(async () => {
                    const currentMutable = this.currentSegment
                    this.otherSegments.push(currentMutable)
                    this.currentSegment = new MutableSegment(++this.nextSegmentId, this.fieldsConfig, this.indexConfig, currentMutable.next)
                    return this.schedulePersist(currentMutable, sync)
                })
            } else {
                waitForImmediate().then(() => {
                    this.rwLock.dispatchWrite(() => {
                        const currentMutable = this.currentSegment
                        this.otherSegments.push(currentMutable)
                        this.currentSegment = new MutableSegment(++this.nextSegmentId, this.fieldsConfig, this.indexConfig, currentMutable.next)
                        this.schedulePersist(currentMutable)
                    })
                })
            }
        } else {
            return Promise.resolve()
        }

    }

    private scheduleNextMerge() {
        /*
        TODO implement segments merging
        if (this.otherSegments.length > 1) {
            const twoSmallist = Array.from(this.otherSegments)
                .sort((a, b) => a.rangeSize - b.rangeSize)
                .slice(0, 2)

            setImmediate(() => {
                // scheduleNextMerge()
            })
        }*/
    }

    private async schedulePersist(currentMutable: MutableSegment, sync: boolean = false): Promise<void> {
        if (sync) {
            const immutable = await ImmutableSegment.fromMutable(currentMutable)

            return this.rwLock.dispatchWrite(() => {
                for (let i = 0; i < this.otherSegments.length; i++) {
                    const s = this.otherSegments[i]
                    if (s.id === immutable.id) {
                        this.otherSegments[i] = immutable
                        console.log("committed")
                        break;
                    }
                }

            })
        } else {
            waitForImmediate().then(async () => this.schedulePersist(currentMutable, true))
        }

    }

    private async persist(segment: MutableSegment): Promise<BaseSegment> {
        return ImmutableSegment.fromMutable(segment);
    }


    private async* currentSegments(): AsyncGenerator<SegmentRange> {
        const listOfSegments = new Array<SegmentRange>()

        await this.rwLock.dispatchRead(() => {
            listOfSegments.push(...this.otherSegments.map(s => ({
                segment: s,
                from: 0,
                count: s.next
            })))
            listOfSegments.push({
                segment: this.currentSegment,
                from: 0,
                count: this.currentSegment.next
            })
        })

        for (const s of listOfSegments) yield s
    }


}


// ----------------------------

export type ProjectionsAndComparator = {
    projections: Array<FieldName>
    comparator: ICompareFunction<ResultItem<Doc>>
}

export function buildComparatorAndProjections(sortClauses: SortClause[]): ProjectionsAndComparator {
    const sortProjection = new Array<FieldName>()

    const comparators = new Array<ICompareFunction<ResultItem<Doc>>>()
    for (const clause of sortClauses) {
        let field: FieldName
        let dir

        if (typeof clause === 'string') {
            field = clause
            dir = SortDirection.ASCENDING
        } else {
            field = clause.field
            dir = clause.dir == SortDirection.DESCENDING ? SortDirection.DESCENDING : SortDirection.ASCENDING
        }


        let fieldComparator: ICompareFunction<ResultItem<Doc>>

        //if (config.flags & FieldConfigFlag.STORED) {
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

        if (dir === SortDirection.DESCENDING) {
            fieldComparator = reverseCompareFunction(fieldComparator)
        }

        comparators.push(fieldComparator)
    }

    const compare = (a: ResultItem<Doc>, b: ResultItem<Doc>): number => {
        for (const comp of comparators) {
            const v = comp(a, b);
            if (v != 0) return -v
        }
        return 0
    }
    return {
        projections: sortProjection,
        comparator: compare
    }
}

