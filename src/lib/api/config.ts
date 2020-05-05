import {Doc, FieldName, FieldStorableValue, FieldValue, FieldValues} from "./base"

export type IndexConfig = {
    defaultFieldConfig: FieldConfig
    storeSourceDoc: boolean
    allFieldConfig: FieldConfig
}

export type FieldsConfig = Record<FieldName, FieldConfig>

export type FieldConfig = {
    flags: FieldConfigFlagSet
    addToAllField?: boolean
    analyzer?: Analyzer
    generator?: ValueGenerator
}

export type FieldConfigFlagSet = number
export enum FieldConfigFlag {
    IGNORED = 0,
    SEARCHABLE = 1 << 0,
    STORED = 1 << 1,
    SORT_OPTIMIZED = 1 << 2
}

export type Analyzer = (input: FieldValue) => Array<FieldValue>
export type ValueGenerator = (input: Doc) => (FieldValue | FieldValues | FieldStorableValue)

