import {Analyzer} from '../api/config'
import {FieldValue} from "../api/base"
import * as moo from 'moo'


export function mooTokenizer(rules: moo.Rules): Analyzer {
    const lexer = moo.compile(rules)

    return input => {
        if (typeof input === 'string') {
            const tokens = new Array<FieldValue>()
            for (const token of lexer.reset(input)) {
                if (token.type === 'TOKEN') {
                    tokens.push(token.text)
                }
            }
            return tokens
        } else {
            return [input]
        }
    }
}
