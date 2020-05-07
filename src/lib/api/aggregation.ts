import {AggregateResult, AggregationName, Doc, FieldName, FieldStorableValue, FieldValue, FieldValues} from "./base"
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

export interface TopAggregateResult extends AggregateResult {
    value: FieldValue
        | FieldValues
        | FieldStorableValue
        | Doc
        | undefined
}

export interface LastAggregateResult extends TopAggregateResult {
    aggregation: LastAggregation
}


export interface FirstAggregation extends TopAggregation {
    readonly name: AggregationName.FIRST
}

export interface FirstAggregateResult extends TopAggregateResult {
    aggregation: FirstAggregation
}


export interface CountDocAggregation {
    readonly name: AggregationName.COUNT
}

export interface CountDocAggregateResult extends AggregateResult {
    aggregation: CountDocAggregation
    count: number
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


export function projectLast(sort: Array<SortClause>, projections?: FieldName[]): LastAggregation {
    return {
        name: AggregationName.LAST,
        sort: sort,
        projections: projections
    }
}

export function projectFirst(sort: Array<SortClause>, projections?: FieldName[]): FirstAggregation {
    return {
        name: AggregationName.FIRST,
        sort: sort,
        projections: projections
    }
}
export function projectDocCount(): CountDocAggregation {
    return {
        name: AggregationName.COUNT,
    }
}
