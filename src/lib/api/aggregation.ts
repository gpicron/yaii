import {AggregationName, FieldName} from "./base"

export interface Aggregation {
    readonly name: AggregationName
}

export interface LastAggregation {
    readonly name: AggregationName.LAST
    readonly field?: FieldName
}

export interface FirstAggregation {
    readonly name: AggregationName.FIRST
    readonly field?: FieldName
}

export interface CountDocAggregation {
    readonly name: AggregationName.COUNT
}

//eslint-disable-next-line
export function isAggregation(v: any): v is Aggregation {
    return isCountDocAggregation(v) ||
        isLastAggregation(v) ||
        isFirstAggregation(v)
}
//eslint-disable-next-line
export function isLastAggregation(v: any): v is Aggregation {
    return v.name == AggregationName.LAST
}
//eslint-disable-next-line
export function isFirstAggregation(v: any): v is Aggregation {
    return v.name == AggregationName.FIRST
}
//eslint-disable-next-line
export function isCountDocAggregation(v: any): v is Aggregation {
    return v.name == AggregationName.COUNT
}


export function projectLast(field?: FieldName): LastAggregation {
    return {
        name: AggregationName.LAST,
        field: field
    }
}

export function projectFirst(field?: FieldName): FirstAggregation {
    return {
        name: AggregationName.FIRST,
        field: field
    }
}
export function projectDocCount(): CountDocAggregation {
    return {
        name: AggregationName.COUNT,
    }
}
