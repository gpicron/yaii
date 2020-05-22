import {Exp} from "./base"
import {FieldName} from "../../api/base"
import {BaseSegment} from "../segments/segment"
import {DocIdIterable} from "../datastructs/docid-async-iterable/base"

export type Term = string

export class TermExp extends Exp {
    readonly field: FieldName
    readonly term: Term

    constructor(field: FieldName, term: Term) {
        super()
        this.field = field
        this.term = term
    }

    toString(): string {
        return `${this.field}:${this.term}`
    }

    async resolve(segment: BaseSegment): Promise<DocIdIterable> {
        return segment.get(this.field, this.term)
    }
}
