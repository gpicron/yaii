import { Analyzer} from '../api/config'
import {FieldValue} from "../api/base"

const defaultSeparator = /[\s\-,;:.]+/g

function STANDARD_TOKENIZER(input: FieldValue): Array<FieldValue> {
    if (typeof input === 'string') {
        const tokens = input
            .trim()
            .toLowerCase()
            .split(defaultSeparator)

        return tokens
    } else {
        return [input]
    }
}

export function standardTokenizer(): Analyzer {
    return STANDARD_TOKENIZER
}
