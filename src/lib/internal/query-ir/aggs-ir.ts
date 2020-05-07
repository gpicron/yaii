import {MutableSegment} from "../mutable-segment"
import {AggregateResult, Doc, ResultItem} from "../../api/base"
import {
    Aggregation,
    TopAggregateResult,
    CountDocAggregateResult,
    isCountDocAggregation,
    isFirstAggregation,
    isLastAggregation, TopAggregation
} from "../../api/aggregation"
import {AggregateProcessor} from "./base"

import {BitmapDocidAsyncIterable} from "../datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {AsyncIterableX, count} from "ix/asynciterable"
import {SingletonDocidAsyncIterable} from "../datastructs/docid-async-iterable/singleton-docid-async-iterable"
import {buildComparatorAndProjections} from "../datastructs/docid-async-iterable/sorted-docid-async-iterable"
import {ICompareFunction, reverseCompareFunction} from "../utils"
import * as op from "ix/asynciterable/operators"
import {MonoTypeOperatorAsyncFunction} from "ix/interfaces"


type TopTask = {
    index: number
    comparator: ICompareFunction<ResultItem<Doc>>
    aggregation: TopAggregation
}

interface IntermediateTopAggregateResult extends AggregateResult {
    aggregation: TopAggregation
    current: ResultItem<Doc>
}

export function buildAggregateExpression(aggregations: Array<Aggregation>, segment: MutableSegment): AggregateProcessor {

    const results = new Array<AggregateResult>(aggregations.length)

    const resultIndexOfCount = new Array<number>()
    const sortProjections = new Set<string>()

    const topTasks = new Array<TopTask>()

    for (let i = 0; i < aggregations.length; i++) {
        const agg = aggregations[i]
        if (isCountDocAggregation(agg)) {
            resultIndexOfCount.push(i)
        } else if (isFirstAggregation(agg)) {
            const cAndP = buildComparatorAndProjections(agg.sort, segment)

            cAndP.projections.forEach(it => sortProjections.add(it))

            topTasks.push({
                index: i,
                comparator:cAndP.comparator,
                aggregation: agg
            })
        } else if (isLastAggregation(agg)) {
            const cAndP = buildComparatorAndProjections(agg.sort, segment)

            cAndP.projections.forEach(it => sortProjections.add(it))

            topTasks.push({
                index: i,
                comparator: reverseCompareFunction(cAndP.comparator),
                aggregation: agg
            })
        }
    }

    const taps: MonoTypeOperatorAsyncFunction<ResultItem<Doc>>[] = topTasks.map(t => op.tap((r: ResultItem<Doc>) => {
        const current = results[t.index] as IntermediateTopAggregateResult
        if (current == undefined) {
            results[t.index] = {
                aggregation: aggregations[t.index],
                current: r
            } as IntermediateTopAggregateResult
        } else if (t.comparator(r, current.current) < 0) {
            current.current = r
        }
    }))


    return async (docIds: AsyncIterableX<number>) => {

        let countResolved = false

        if (resultIndexOfCount.length > 0) {
            if (SingletonDocidAsyncIterable.is(docIds)) {
                resultIndexOfCount.forEach(index => results[index] = {
                    aggregation: aggregations[index],
                    count: 1
                } as CountDocAggregateResult)

                countResolved = true
            } else if (BitmapDocidAsyncIterable.is(docIds)) {
                const size = docIds.size

                resultIndexOfCount.forEach(index => results[index] = {
                    aggregation: aggregations[index],
                    count: size
                } as CountDocAggregateResult)

                countResolved = true
            }
        } else {
            countResolved = true
        }



        if (countResolved && topTasks.length == 0) {
            return Promise.resolve(results)
        } else {
            let docs;
            if (taps.length > 0) {
                if (sortProjections.size > 0) {
                    docs = segment.project<Doc>(docIds, Array.from(sortProjections))
                } else {
                    docs = docIds.pipe(op.map(it => ({_id: it})))
                }

                docs = docs.pipe(
                    ...taps
                )

            } else {
                docs = docIds
            }

            const total = await count(docs)

            if (!countResolved) {
                resultIndexOfCount.forEach(index => results[index] = {
                    aggregation: aggregations[index],
                    count: total
                } as CountDocAggregateResult)
            }

            for (const t of topTasks) {
                const ir = results[t.index] as IntermediateTopAggregateResult
                results[t.index] = {
                    aggregation: ir.aggregation,
                    value: segment.projectDoc(ir.current._id, ir.aggregation.projections)
                } as TopAggregateResult
            }

            return Promise.resolve(results)
        }
    }


}


