import {AggregateResults, BucketType, FieldName, FieldValue} from "./base"


export interface BucketSpecification<K> {
    readonly type: BucketType
}

export interface TermBuckets extends BucketSpecification<FieldValue> {
    readonly type: BucketType.TERM
    readonly field: FieldName
}

export interface NumericBuckets extends BucketSpecification<number> {
    readonly type: BucketType.NUMERIC
    readonly field: FieldName
    readonly bucketSize: number
}

export interface BucketAggregateResults<K> extends AggregateResults {
    readonly spec: BucketSpecification<K>
    readonly bucket: K
}


export function bucketTerm(field: FieldName): TermBuckets {
    return {
        type: BucketType.TERM,
        field: field
    }
}

