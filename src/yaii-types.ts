import { AsyncIterableX as AsyncIterable } from 'ix/asynciterable'
import { Query } from './lib/query'
import { standardTokenizer } from './lib/analyzer/standard-tokenizer'
import { stopwordFilter } from './lib/analyzer/stopwords-filter'
import { DocId } from './lib/utils'

export * from './lib/query'

export interface InvertedIndex {
    add(input: Doc | AsyncIterable<Doc>): Promise<number>

    query(
        filter: Query,
        projection?: Array<FieldName>,
        limit?: number,
        sort?: Array<SortClause>
    ): AsyncIterable<ResultItem>

    listAllKnownField(): Record<string, FieldConfig>
}

// -----------------------------------

export type FieldsConfig = Record<FieldName, FieldConfig>

export type FieldConfig = {
    flags: FieldConfigFlagSet
    addToAllField?: boolean
    analyzer?: Analyzer
    generator?: ValueGenerator
}

export type FieldConfigFlagSet = number

export enum FieldConfigFlag {
    SEARCHABLE = 1 << 0,
    STORED = 1 << 1,
    SORT_OPTIMIZED = 1 << 2
}

export type Analyzer = (input: FieldValue) => Array<FieldValue>
export type ValueGenerator = (input: Doc) => Array<FieldValue>

export type IndexConfig = {
    defaultFieldConfig: FieldConfig
    storeSourceDoc: boolean
    allFieldConfig: FieldConfig
}

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

// -----------------------------------

export enum SortDirection {
    ASCENDING = 'ASC',
    DESCENDING = 'DESC'
}

export type SortClause =
    | {
          field: FieldName
          dir?: SortDirection
      }
    | FieldName

// ----------------------------------------------

export type FieldName = string
export type FieldValue = TokenValue | TextValue | NumericFieldValue
export type FieldValues = Array<FieldValue>

export type FieldStorableValue = Buffer

export type NumericFieldValue = IntegerValue
export type TokenValue = string | boolean
export type TextValue = string
export type IntegerValue = number

interface StoredFields {
    [paramKey: string]:
        | FieldValue
        | FieldValues
        | FieldStorableValue
        | Doc
        | undefined
}

export interface ResultItem extends StoredFields {
    _id: DocId
    _source?: Doc
}

export interface Doc {
    [paramKey: string]:
        | FieldValue
        | FieldValues
        | FieldStorableValue
        | Doc
        | Array<Doc>
}

// ----------------------------------------------

export function isFieldValue(obj: unknown): obj is FieldValue {
    return typeof obj === 'string' || typeof obj === 'number'
}

export function isNumericFieldValue(obj: unknown): obj is NumericFieldValue {
    return typeof obj === 'number' && Number.isSafeInteger(obj)
}

export function isFieldValues(obj: unknown): obj is FieldValues {
    return Array.isArray(obj) && obj.every(isFieldValue)
}
