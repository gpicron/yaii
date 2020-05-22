import {DocId, FieldName, FieldStorableValue, FieldValue, FieldValues, isFieldValue, isIntegerValue} from '../api/base'
import {Term} from './query-ir/term-exp'
import {FieldConfigFlagSet} from "../api/config"
import {BaseSegment} from "./segments/segment"
import {DocIdIterable} from "./datastructs/docid-async-iterable/base"
import {RangeDocidAsyncIterable} from "./datastructs/docid-async-iterable/range-docid-async-iterable"
import {andMany, andNot} from "./datastructs/docid-async-iterable/operations"

export type ExtFieldsIndexConfig = Record<FieldName, ExtFieldConfig>

export type Termizer = (val: FieldValue | FieldValues) => Array<Term>

export type ExtFieldConfig = {
    flags: FieldConfigFlagSet
    readonly all: boolean
    tokenizer: Termizer
}
export type ExtIndexConfig = {
    defaultFieldConfig: ExtFieldConfig
    storeSourceDoc: boolean
    allFieldConfig: ExtFieldConfig
    storePath: string
}

export function removeAllFromSet<T>(originalSet: Set<T>, toBeRemovedSet: Set<T>): void {
    ;[...toBeRemovedSet].forEach(function(v) {
        originalSet.delete(v)
    })
}

export type ICompareFunction<T> = (a: T, b: T) => number
export type IEqualsFunction<T> = (a: T, b: T) => boolean

export enum INTERNAL_FIELDS {
    FIELDS = '£_FIELD',
    ALL = '£_ALL',
    SOURCE = '£_SOURCE',
    FILTER_CACHE = '£_FILTER_CACHE'
}

export const REFERENCE_COLLATOR_COMPARATOR = new Intl.Collator(["en", 'fr', 'de'], {
    caseFirst: 'lower',
    ignorePunctuation: false,
    sensitivity: 'base',
    usage: 'sort'
}).compare


export function reverseCompareFunction<T>(f: ICompareFunction<T>): ICompareFunction<T> {
    return (a: T, b: T) => -f(a, b)
}

export function flattenObject(ob: Record<string, unknown>): Record<FieldName, FieldValue | FieldValues | FieldStorableValue> {
    const toReturn: Record<FieldName, FieldValue | FieldValues | FieldStorableValue> = {}

    for (const i in ob) {
        if (!ob.hasOwnProperty(i)) continue

        const obElement = ob[i]
        if (typeof obElement === 'object') {
            if (Array.isArray(obElement)) {
                if (obElement.every(isFieldValue)) {
                    toReturn[i] = obElement
                } else {
                    const flattenedChilds = obElement.map(flattenObject)
                    for (const child of flattenedChilds) {
                        for (const x in child) {
                            if (!child.hasOwnProperty(x)) continue

                            const key = `${i}.${x}`
                            const existing: FieldValue | FieldValues | FieldStorableValue = toReturn[key]
                            const childElement: FieldValue | FieldValues | FieldStorableValue = child[x]

                            if (existing) {
                                if (Array.isArray(existing)) {
                                    if (Array.isArray(childElement)) {
                                        toReturn[key] = existing.concat(childElement)
                                    } else if (Buffer.isBuffer(childElement)) {
                                        throw new Error('Document contains multiple Buffer for a given path.  This is not supported')
                                    } else if (childElement !== null && childElement !== undefined) {
                                        existing.push(childElement)
                                    }
                                } else if (Buffer.isBuffer(existing)) {
                                    throw new Error('Document contains multiple Buffer + values for a given path.  This is not supported')
                                } else if (childElement != null && childElement != undefined) {
                                    if (Array.isArray(childElement)) {
                                        childElement.unshift(existing)
                                        toReturn[key] = childElement
                                    } else if (Buffer.isBuffer(childElement)) {
                                        throw new Error('Document contains multiple Buffer + values for a given path.  This is not supported')
                                    } else if (childElement !== null && childElement !== undefined) {
                                        toReturn[key] = [existing, childElement]
                                    }
                                }
                            } else if (childElement != null && childElement != undefined) {
                                toReturn[key] = childElement
                            }
                        }
                    }
                }
            } else if (obElement !== null) {
                const flatObject = flattenObject(obElement as Record<string, unknown>)
                for (const x in flatObject) {
                    if (!flatObject.hasOwnProperty(x)) continue

                    toReturn[`${i}.${x}`] = flatObject[x]
                }
            }
        } else if (isFieldValue(obElement)) {
            toReturn[i] = obElement
        } else if (typeof obElement === 'number') {
            const floor = Math.floor(obElement)
            if (isIntegerValue(floor)) {
                toReturn[i] = floor
            }
        }
    }

    return toReturn
}

export function opinionatedCompare(a: FieldValue | undefined | Buffer, b: FieldValue | undefined | Buffer): number {
    switch (typeof a) {
        case 'undefined':
            switch (typeof b) {
                case 'undefined':
                    return 0
                default:
                    return -1
            }
        case 'boolean':
            switch (typeof b) {
                case 'undefined':
                    return 1
                case 'boolean':
                    return a ? b ? 0 : -1 : b ? 1 : 0
                default:
                    return -1
            }
        case 'number':
            switch (typeof b) {
                case 'undefined':
                case 'boolean':
                    return 1
                case 'number':
                    return a - b
                default:
                    return -1
            }
        case 'string':
            switch (typeof b) {
                case 'undefined':
                case 'boolean':
                case 'number':
                    return 1
                case 'string':
                    return REFERENCE_COLLATOR_COMPARATOR(a, b)
            }
        case 'object':
            switch (typeof b) {
                case 'undefined':
                case 'boolean':
                case 'number':
                case 'string':
                    return 1
                case 'object':
                    const bbA = a as Buffer
                    const aLen = bbA.length
                    const bbB = b
                    const bLen = bbB.length
                    const min = Math.min(aLen, bLen)
                    for (let i = 0; i < min; i++) {
                        const comp = bbA.readUInt8(i) - bbB.readUInt8(i)
                        if (comp != 0) return comp
                    }

                    return aLen - bLen
                default:
                    throw new Error('bug')
            }
        default:
            throw new Error('bug')
    }
}

export function assertUnreachable(x: never): never {
    throw new Error(`Didn't expect to get here ${x}`);
}

export async function waitForImmediate(): Promise<void> {
    return new Promise(resolve => {
        setImmediate(() => resolve() )
    })
}

export type SegmentRange = {
    segment: BaseSegment,
    from: DocId,
    count: number
}
export function removeDeletedAndAddedAfter(docIds: DocIdIterable, segmentRange: SegmentRange): DocIdIterable {
    const deleted = segmentRange.segment.deleted()
    const first = segmentRange.from
    const count = segmentRange.count
    if (RangeDocidAsyncIterable.is(docIds)) {
        if (deleted.cost === 0) {
            return andMany([docIds, new RangeDocidAsyncIterable(first, count)])
        } else {
            return deleted.clone().removeRange(0, first).flipRange(first, first + count).removeRange(first + count)
        }
    } else {
        const docIdIterable = andMany([docIds, new RangeDocidAsyncIterable(segmentRange.from, segmentRange.count)])
        if (docIdIterable) {
            return andNot(docIdIterable, deleted)
        } else {
            throw new Error("bug")
        }
    }
    return docIds
}
