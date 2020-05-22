import {AggregateResult, AggregateResults, AggregationName, Doc, FieldName, ResultItem} from "./base"
import {SortClause} from "./query"

export interface Aggregation {
    readonly name: AggregationName
}

export interface TopAggregation extends Aggregation {
    readonly sort: Array<SortClause>
    readonly projections?: FieldName[]
}

export interface LastAggregation extends TopAggregation {
    readonly name: AggregationName.LAST
}

export type TopAggregateResult<T extends Doc> = AggregateResult<ResultItem<T> | undefined>

export interface LastAggregateResult<T extends Doc> extends TopAggregateResult<T> {
    aggregation: LastAggregation
}


export interface FirstAggregation extends TopAggregation {
    readonly name: AggregationName.FIRST
}

export interface FirstAggregateResult<T extends Doc> extends TopAggregateResult<T> {
    aggregation: FirstAggregation
}


export interface CountDocAggregation {
    readonly name: AggregationName.COUNT
}

export interface CountDocAggregateResult extends AggregateResult<number> {
    aggregation: CountDocAggregation
}


export interface GroupByAggregation {
    readonly name: AggregationName.GROUP_BY
    readonly fieldName: FieldName
    readonly aggregations: Aggregation[]
}

export interface GroupByAggregateResult extends AggregateResult<Map<string|number|undefined,AggregateResults>> {
    aggregation: GroupByAggregation
}



//eslint-disable-next-line
export function isAggregation(v: any): v is Aggregation {
    return isCountDocAggregation(v) ||
        isLastAggregation(v) ||
        isFirstAggregation(v)
}
//eslint-disable-next-line
export function isTopAggregation(v: any): v is TopAggregation {
    return isLastAggregation(v) || isFirstAggregation(v)
}
//eslint-disable-next-line
export function isLastAggregation(v: any): v is LastAggregation {
    return v.name == AggregationName.LAST
}
//eslint-disable-next-line
export function isFirstAggregation(v: any): v is FirstAggregation {
    return v.name == AggregationName.FIRST
}
//eslint-disable-next-line
export function isCountDocAggregation(v: any): v is CountDocAggregation {
    return v.name == AggregationName.COUNT
}

//eslint-disable-next-line
export function isGroupByAggregation(v: any): v is GroupByAggregation {
    return v.name == AggregationName.GROUP_BY
}


export function aggrLast(sort: Array<SortClause>, projections?: FieldName[]): LastAggregation {
    return {
        name: AggregationName.LAST,
        sort: sort,
        projections: projections
    }
}

export function aggrFirst(sort: Array<SortClause>, projections?: FieldName[]): FirstAggregation {
    return {
        name: AggregationName.FIRST,
        sort: sort,
        projections: projections
    }
}

export function aggrCount(): CountDocAggregation {
    return {
        name: AggregationName.COUNT,
    }
}

export function aggrGroupBy(fieldName: FieldName, aggregations: Aggregation[]): GroupByAggregation {
    return {
        name: AggregationName.GROUP_BY,
        fieldName: fieldName,
        aggregations: aggregations
    }
}
