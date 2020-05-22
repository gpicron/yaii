/* eslint-disable @typescript-eslint/ban-ts-ignore */
import {RoaringBitmap32} from 'roaring'
import {Doc, DocId, FieldValue, ResultItem} from '../../api/base'
import {ICompareFunction, REFERENCE_COLLATOR_COMPARATOR} from '../utils'
import ByteBuffer = require('bytebuffer')
import Long = require('long')


class ComparisonKeyCache {
    readonly id: DocId
    readonly pointer: number
    readonly prefix: number

    private numberValue: number | undefined
    private stringValue: string | undefined

    constructor(id: DocId, pointers: RoaringBitmap32, store: ByteBuffer) {
        this.id = id
        this.pointer = pointers.select(id - 1) || 0
        this.prefix = store.readByte(this.pointer)
    }

    getNumberValue(store: ByteBuffer): number {
        if (this.numberValue) return this.numberValue
        this.numberValue = store.readUint64(this.pointer).shiftLeft(8).shiftRightUnsigned(8).toNumber()
        return this.numberValue
    }
    getStringValue(pointers: RoaringBitmap32, store: ByteBuffer): string {
        if (this.stringValue) return this.stringValue

        const endPointer = pointers.select(this.id) || store.offset
        const len = endPointer - this.pointer - 1

        // @ts-ignore  (bug in @types, second param is a string)
        this.stringValue = store.readUTF8String(len, 'b', this.pointer+1) as string

        return this.stringValue
    }


}


export class SortFieldPackedArray {
    private id: string
    private cacheField: string
    private pointers = new RoaringBitmap32()
    private store = ByteBuffer.allocate(4 * 1024, false, true)

    constructor(id: string) {
        this.id = id
        this.cacheField = `__cache__${id}`
    }

    add(value: FieldValue): void {
        switch (typeof value) {
            case 'undefined':
                this.store.writeUint8(0x00)
                break
            case 'boolean':
                this.store.writeUint8(value ? 0x01 : 0x02)
                break
            case 'number': // supposed to be a safe integer
                const long = Long.fromNumber(value).add(Number.MAX_SAFE_INTEGER)
                const bytes = long.toBytesBE()

                // we need only 7 bytes to code a safe integer
                this.store.writeUint8(0x03)
                for (let i = 1; i < 8; i++) {
                    this.store.writeUint8(bytes[i])
                }
                break
            case 'string':
                this.store.writeUint8(0x04)
                this.store.writeUTF8String(value)
                break
            default:
                throw new Error('datatype not supported')
        }
        this.pointers.add(this.store.offset)
    }



    comparator: ICompareFunction<ResultItem<Doc>> = (a: ResultItem<Doc>, b: ResultItem<Doc>) => {
        const aId = a._id
        let aCache = a[this.cacheField] as unknown as ComparisonKeyCache
        if (!aCache) {
            aCache = new ComparisonKeyCache(aId, this.pointers, this.store)
            // @ts-ignore
            a[this.cacheField] = aCache
        }

        const bId = b._id
        let bCache = b[this.cacheField] as unknown as ComparisonKeyCache
        if (!bCache) {
            bCache = new ComparisonKeyCache(bId, this.pointers, this.store)
            // @ts-ignore
            b[this.cacheField] = bCache
        }

        const s = this.store

        const compPrefix = aCache.prefix - bCache.prefix

        if (compPrefix != 0) return compPrefix

        switch (aCache.prefix) {
            case 0x00:
            case 0x01:
            case 0x02:
                return 0
            case 0x03:

                return aCache.getNumberValue(s) - bCache.getNumberValue(s)
            case 0x04:
                const aString = aCache.getStringValue(this.pointers, s)
                const bString = aCache.getStringValue(this.pointers, s)

                return REFERENCE_COLLATOR_COMPARATOR(aString, bString)
            default:
                throw new Error('bug')
        }
    }
}

