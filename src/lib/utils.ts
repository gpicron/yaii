import {
    FieldConfigFlagSet,
    FieldName,
    FieldValue,
    FieldValues,
    isFieldValue
} from '../yaii-types'
import { Term } from './query-ir'
import { arrayDestination, encodeUTF16toUTF8, stringSource } from 'utfx'

import * as assert from 'assert'

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
}

export type DocId = number // actually a 32-bit integer

export function removeAll<T>(
    originalSet: Set<T>,
    toBeRemovedSet: Set<T>
): void {
    ;[...toBeRemovedSet].forEach(function(v) {
        originalSet.delete(v)
    })
}

export type ICompareFunction<T> = (a: T, b: T) => number
export type IEqualFunction<T> = (a: T, b: T) => boolean

export function reverseCompareFunction<T>(
    f: ICompareFunction<T>
): ICompareFunction<T> {
    return (a: T, b: T) => -f(a, b)
}

export function flattenObject(
    ob: Record<string, unknown>
): Record<string, FieldValues> {
    const toReturn: Record<string, FieldValues> = {}

    assert.ok(typeof ob === 'object' && !Array.isArray(ob))

    for (const i in ob) {
        if (!ob.hasOwnProperty(i)) continue

        const obElement = ob[i]
        if (typeof obElement === 'object') {
            if (Array.isArray(obElement)) {
                if (obElement.every(isFieldValue)) {
                    toReturn[i] = obElement
                } else {
                    const flattenedChilds: Record<
                        string,
                        FieldValues
                    >[] = obElement.map(flattenObject)
                    for (const child of flattenedChilds) {
                        for (const x in child) {
                            if (!child.hasOwnProperty(x)) continue
                            const key = `${i}.${x}`
                            const array: FieldValues = toReturn[key]
                            if (array) {
                                toReturn[key] = array.concat(child[x])
                            } else {
                                toReturn[key] = child[x]
                            }
                        }
                    }
                }
            } else if (obElement !== null) {
                const flatObject = flattenObject(
                    obElement as Record<string, unknown>
                )
                for (const x in flatObject) {
                    if (!flatObject.hasOwnProperty(x)) continue

                    toReturn[`${i}.${x}`] = flatObject[x]
                }
            }
        } else if (isFieldValue(obElement)) {
            toReturn[i] = [obElement]
        }
    }

    return toReturn
}

export function opinionatedCompare(
    a: FieldValue | undefined | Buffer,
    b: FieldValue | undefined | Buffer
): number {
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
                    return a ? (b ? 0 : -1) : b ? 1 : 0
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
                    const aBytes = new Array<number>(0)
                    let comp = 0
                    encodeUTF16toUTF8(stringSource(a), arrayDestination(aBytes))
                    encodeUTF16toUTF8(stringSource(b), function(bByte: number) {
                        if (comp == 0) {
                            const aByte = aBytes.shift()
                            if (aByte) {
                                comp = aByte - bByte
                            } else {
                                comp = -1
                            }
                        }
                    })
                    if (comp == 0 && aBytes.length > 0) comp = 1

                    return comp
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