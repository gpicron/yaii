export type FieldName = string
export type FieldValue = TokenValue | IntegerValue
export type FieldValues = Array<FieldValue>
export type FieldStorableValue = Buffer | StorableNumberValue | FieldValue | FieldValues
export type TokenValue = string | boolean
export type IntegerValue = number
export type StorableNumberValue = number

export interface Doc {
    [paramKey: string]:
        | FieldValue
        | FieldValues
        | FieldStorableValue
        | Doc
        | Array<Doc>
        | undefined
        | null
}


export function isFieldValue(obj: unknown): obj is FieldValue {
    return typeof obj === 'string' || typeof obj === 'boolean' || isIntegerValue(obj)
}

export function isIntegerValue(obj: unknown): obj is IntegerValue {
    return typeof obj === 'number' && Number.isSafeInteger(obj)
}

export function isFieldValues(obj: unknown): obj is FieldValues {
    return Array.isArray(obj) && obj.every(isFieldValue)
}

// ---

export type DocId = number

interface StoredFields {
    [paramKey: string]:
        | FieldValue
        | FieldValues
        | FieldStorableValue
        | Doc
        | undefined
}

export interface ResultItem<T extends Doc> extends StoredFields {
    _id: DocId
    _source?: T
}

export enum QueryOperator {
    TOKEN = 'TOKEN',
    TOKEN_RANGE = 'TOKEN_RANGE',
    NUMBER = 'NUMBER',
    NUMBER_RANGE = 'NUMBER_RANGE',
    TEXT_CONTAINS = 'TEXT_CONTAINS',
    ALL = 'ALL',
    OR = 'OR',
    AND = 'AND',
    NOT = 'NOT',
    HAS_FIELD = 'HAS_FIELD',
    BUCKET = 'BUCKET'
}

export enum AggregationName {
    LAST = 'LAST',
    FIRST = 'FIRST',
    COUNT = 'FIRST',
}
