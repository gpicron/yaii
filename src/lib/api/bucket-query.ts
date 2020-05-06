import {FieldName, QueryOperator} from "./base"
import {Query} from "./query"

export type BucketKeyGenerator =  TermKeyGenerator

export interface TermKeyGenerator {
    readonly field: FieldName
}

export interface BucketQuery extends Query {
    readonly operator: QueryOperator.BUCKET
    readonly query: Query
    readonly buckets: BucketKeyGenerator
}
