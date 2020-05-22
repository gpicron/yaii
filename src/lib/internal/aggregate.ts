import {
    Aggregation,
    CountDocAggregateResult,
    CountDocAggregation,
    GroupByAggregateResult,
    GroupByAggregation,
    TopAggregateResult,
    TopAggregation
} from "../api/aggregation"
import {AggregateResult, AggregateResults, Doc, DocId, FieldName, ResultItem} from "../api/base"
import {ICompareFunction, SegmentRange} from "./utils"
import {SortClause} from "../api/query"
import {BaseSegment} from "./segments/segment"
import {buildComparatorAndProjections} from "../../base-inverted-index"

export abstract class AggrAccumulator<A extends Aggregation, RESULT> {
    readonly aggregation: A

    constructor(aggregation: A) {
        this.aggregation = aggregation
    }

    abstract accumulate(item: ResultItem<Doc>): void

    abstract finalize(count: number): Promise<RESULT>
}

export abstract class AggrAccumulatorMerger<A extends Aggregation, ACC_RESULT, RESULT extends AggregateResult<unknown>> {
    readonly aggregation: A

    constructor(aggregation: A) {
        this.aggregation = aggregation
    }

    abstract accumulate(item: ACC_RESULT): void

    abstract finalize(): Promise<RESULT | undefined>
}


export interface TopAggregateResultForSegment extends TopAggregateResult<ResultItem<Doc>> {
    segment: BaseSegment
}

export class TopAccumulator extends AggrAccumulator<TopAggregation, TopAggregateResultForSegment> {

    comparator: ICompareFunction<ResultItem<Doc>>
    current: ResultItem<Doc> | undefined
    range: SegmentRange


    constructor(comparator: ICompareFunction<ResultItem<Doc>>, range: SegmentRange, aggregation: TopAggregation) {
        super(aggregation)
        this.comparator = comparator
        this.range =  range
    }

    accumulate(item: ResultItem<Doc>): void {
        if (this.current == undefined || this.comparator(item, this.current) < 0) {
            this.current = item
        }
    }

    async finalize(): Promise<TopAggregateResultForSegment> {
        const projections = this.aggregation.sort.map((sortClause: SortClause) => typeof sortClause === 'string' ? sortClause :  sortClause.field)
        return {
            aggregation: this.aggregation,
            value: this.current ? await this.range?.segment.projectDoc(this.current?._id, projections) : undefined,
            segment: this.range.segment
        }
    }
}

export class TopAccumulatorMerger extends AggrAccumulatorMerger<TopAggregation, TopAggregateResultForSegment, TopAggregateResult<ResultItem<Doc>>> {
    private current?: TopAggregateResultForSegment
    private comparator: ICompareFunction<ResultItem<Doc>>
    private projections: Array<FieldName>
    private min: boolean


    constructor(aggregation: TopAggregation, min: boolean) {
        super(aggregation)
        const comparatorAndProjections = buildComparatorAndProjections(aggregation.sort)
        this.comparator = comparatorAndProjections.comparator
        this.projections = comparatorAndProjections.projections
        this.min = min
    }

    async accumulate(item: TopAggregateResultForSegment): Promise<void> {
        if (item.value) {
            if (this.current) {
                const v = await item.segment.projectDoc(item.value._id, this.projections)
                if (this.min) {
                    if (this.comparator(this.current.value as ResultItem<Doc>, v) < 0) {
                        this.current = item
                    }
                } else {
                    if (this.comparator(this.current.value as ResultItem<Doc>, v) > 0) {
                        this.current = item
                    }
                }
            } else {
                this.current = item
            }
        }
    }

    async finalize(): Promise<TopAggregateResult<ResultItem<Doc>> | undefined> {
        if (this.current) {
            return {
                aggregation: this.aggregation,
                value: await this.current.segment.projectDoc(this.current.value?._id as DocId, this.aggregation.projections)
            }
        }
    }
}

export class CountAccumulator extends AggrAccumulator<CountDocAggregation, CountDocAggregateResult> {


    accumulate(): void {
        // do nothing.
    }

    async finalize(count: number): Promise<CountDocAggregateResult> {
        return Promise.resolve({
            aggregation: this.aggregation,
            value: count
        });
    }
}

export class CountAccumulatorMerger extends AggrAccumulatorMerger<CountDocAggregation, CountDocAggregateResult, CountDocAggregateResult> {
    current?: CountDocAggregateResult

    accumulate(item: CountDocAggregateResult): void {
        if (this.current) {
            this.current.value += item.value
        } else {
            this.current = item
        }
    }

    async finalize(): Promise<CountDocAggregateResult> {
        if (this.current) {
            return this.current
        } else {
            return {
                aggregation: this.aggregation,
                value: 0
            }
        }
    }

}


interface GroupByGroupAcc {
    count: number
    _buffer: ResultItem<Doc>[]
    accumulators: AggrAccumulator<Aggregation, AggregateResult<unknown>>[]
}

type GroupByAcc =  Map<string | number | undefined, GroupByGroupAcc>

export class GroupByAccumulator extends AggrAccumulator<GroupByAggregation, GroupByAggregateResult> {
    private accumulatorsFactory: () => AggrAccumulator<Aggregation, AggregateResult<unknown>>[]
    private accumulation: GroupByAcc

    constructor(agg: GroupByAggregation, range: SegmentRange, accumulatorsFactory: () => AggrAccumulator<Aggregation, AggregateResult<unknown>>[]) {
        super(agg)
        this.accumulatorsFactory = accumulatorsFactory
        this.accumulation = new Map<string | number | undefined, GroupByGroupAcc>()
    }


    accumulate(item: ResultItem<Doc>): void {
        const group = item[this.aggregation.fieldName]
        if (typeof group === 'string' || typeof group === 'number' || typeof group === 'undefined') {

            let groupByGroupAcc = this.accumulation.get(group)
            if (groupByGroupAcc === undefined) {
                groupByGroupAcc = {
                    count: 0,
                    _buffer: [item],
                    accumulators: this.accumulatorsFactory()
                }
                this.accumulation.set(group, groupByGroupAcc)
            } else {
                const buffer = groupByGroupAcc._buffer
                buffer.push(item)
                if (buffer.length === 1024) this.flush(groupByGroupAcc)
            }
        }
    }

    async finalize(): Promise<GroupByAggregateResult> {
        this.flushBuffer()
        const result = new Map<string | number | undefined, AggregateResults>()

        for (const [group, groupAcc] of this.accumulation.entries()) {
            const groupResults = []
            for (let i = 0; i < groupAcc.accumulators.length; i++) {
                groupResults[i] = await groupAcc.accumulators[i].finalize(groupAcc.count)
            }

            result.set(group, groupResults)
        }

        return {
            aggregation: this.aggregation,
            value: result
        }
    }


    private flushBuffer() {
        this.accumulation.forEach((groupAcc) => this.flush(groupAcc))
    }

    private flush(groupAcc: GroupByGroupAcc) {
        const items = groupAcc._buffer
        groupAcc.count += items.length
        for (let i = 0; i < groupAcc.accumulators.length; i++) {
            const acc = groupAcc.accumulators[i]
            for (const item of items) {
                acc.accumulate(item)
            }

        }
        items.length = 0
    }
}

type MergerGroupByAggregateResult = AggrAccumulatorMerger<Aggregation, unknown, AggregateResult<unknown>>[]

type MergerGroupByAcc =  Map<string | number | undefined, MergerGroupByAggregateResult>


export class GroupByAccumulatorMerger extends AggrAccumulatorMerger<GroupByAggregation, GroupByAggregateResult, GroupByAggregateResult> {
    private mergersFactory: () => AggrAccumulatorMerger<Aggregation, unknown, AggregateResult<unknown>>[]
    private accumulation = new Map<string | number | undefined, MergerGroupByAggregateResult>()

    constructor(agg: GroupByAggregation, mergersFactory: () => AggrAccumulatorMerger<Aggregation, unknown, AggregateResult<unknown>>[]) {
        super(agg)
        this.mergersFactory = mergersFactory

    }
    accumulate(item: GroupByAggregateResult): void {
        item.value.forEach( (aggrResult, group) => {
            let groupMerger = this.accumulation.get(group)

            if (!groupMerger) {
                groupMerger  = this.mergersFactory()
                this.accumulation.set(group, groupMerger)
            }

            for (let i = 0; i < groupMerger.length; i++) {
                groupMerger[i].accumulate(aggrResult[i])
            }
        })
    }

    async finalize(): Promise<GroupByAggregateResult> {
        const result = new Map<string|number|undefined,AggregateResults>()
        for (const [group, mergers] of this.accumulation.entries()) {
            const all = Promise.all(mergers.map(async m => m.finalize() as Promise<AggregateResult<unknown>>))
            result.set(group, await all)
        }

        return {
            value: result,
            aggregation: this.aggregation
        }
    }

}
