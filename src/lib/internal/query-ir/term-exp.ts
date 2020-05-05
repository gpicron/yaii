import {Exp} from "./base"
import {FieldName} from "../../api/base"
import {MutableSegment} from "../mutable-segment"
import {DocidAsyncIterable} from "../datastructs/docid-async-iterable/docid-async-iterable"

export type Term = Buffer

export class TermExp extends Exp {
    readonly field: FieldName
    readonly term: Term

    constructor(field: FieldName, term: Term) {
        super()
        this.field = field
        this.term = term
    }

    toString(): string {
        return `${this.field}:${this.term.toString('hex')}`
    }

    async resolve(segment: MutableSegment): Promise<DocidAsyncIterable> {
        return segment.get(this.field, this.term)
    }
}
