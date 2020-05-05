import { Analyzer} from '../api/config'
import { FieldValue } from "../api/base"

export function lowercaseFilter(tokenizer: Analyzer): Analyzer {
    return (input: FieldValue) => {
        const tokens = tokenizer(input)
        const result = []
        for (const t of tokens) {
            if (typeof t === 'string') {
                result.push(t.toLowerCase())
            } else {
                result.push(t)
            }
        }
        return result
    }
}
