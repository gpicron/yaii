import {Doc, FieldStorableValue, FieldValue, FieldValues} from './lib/api/base'
import {Termizer} from './lib/internal/utils'


export const NULL_TOKENIZER: Termizer = () => {
    throw new Error('Bug, should never be called')
}

export interface IndexableDocWithoutSource {
    [paramKey: string]: FieldValue | FieldValues | FieldStorableValue
}

export type IndexableDoc = IndexableDocWithoutSource & {
    '£_SOURCE'?: Doc
}


export type IndexableDocPipelineMapFunction = (
    input: IndexableDoc
) => Promise<IndexableDoc> | IndexableDoc


/// ------------------------------------------------------
