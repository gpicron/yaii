import {Doc, FieldName, ResultItem} from './lib/api/base'
import {Query, SortClause} from './lib/api/query'
import {FieldConfig, FieldConfigFlag, IndexConfig} from "./lib/api/config"
import {standardTokenizer, stopwordFilter} from "./lib/analyzer"

import { AsyncIterableX } from 'ix/asynciterable'

export * from './lib/api/base'
export * from './lib/api/query'
export * from "./lib/api/query-dsl"
export * from "./lib/api/config"
export * from './lib/analyzer/index'

export interface InvertedIndex {
    add(input: Doc | AsyncIterable<Doc>): Promise<number>

    query<T extends Doc>(filter: Query, sort?: Array<SortClause>, limit?: number, projection?: Array<FieldName>): AsyncIterableX<ResultItem<T>>

    listAllKnownField(): Record<FieldName, FieldConfig>
}

// -----------------------------------

export const DEFAULT_FIELD_CONFIG: FieldConfig = {
    flags: FieldConfigFlag.SEARCHABLE,
    analyzer: standardTokenizer(),
    addToAllField: true
}

export const DEFAULT_ALL_FIELD_CONFIG: FieldConfig = {
    flags: FieldConfigFlag.SEARCHABLE,
    analyzer: stopwordFilter(standardTokenizer())
}

export const DEFAULT_INDEX_CONFIG: IndexConfig = {
    defaultFieldConfig: DEFAULT_FIELD_CONFIG,
    storeSourceDoc: true,
    allFieldConfig: DEFAULT_ALL_FIELD_CONFIG
}




