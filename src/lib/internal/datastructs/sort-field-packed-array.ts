import { RoaringBitmap32 } from 'roaring'
import {DocId, FieldValue} from '../../api/base'
import { ICompareFunction } from '../utils'
import ByteBuffer = require('bytebuffer')
import Long = require('long')

function compareOver(bb: ByteBuffer, aOffset: number, bOffset: number, over: number): number {
    for (let i = 0; i < over; i++) {
        const cmp = bb.readUint8(aOffset) - bb.readUint8(bOffset)
        if (cmp != 0) return cmp
        aOffset++
        bOffset++
    }

    return 0
}

export class SortFieldPackedArray {
    private pointers = new RoaringBitmap32()
    private store = ByteBuffer.allocate(4 * 1024, false, true)

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

    comparator: ICompareFunction<DocId> = (a: DocId, b: DocId) => {
        let aPointer = this.pointers.select(a - 1) || 0
        let bPointer = this.pointers.select(b - 1) || 0

        const s = this.store

        const prefixA = s.readByte(aPointer)
        const prefixB = s.readByte(bPointer)

        const compPrefix = prefixA - prefixB

        if (compPrefix != 0) return compPrefix

        switch (prefixA) {
            case 0x00:
            case 0x01:
            case 0x02:
                return 0
            case 0x03:
                aPointer++
                bPointer++
                return compareOver(s, aPointer, bPointer, 7)
            case 0x04:
                const aEndPointer = this.pointers.select(a) || s.offset
                const bEndPointer = this.pointers.select(b) || s.offset
                aPointer++
                bPointer++
                const aLen = aEndPointer - aPointer
                const bLen = bEndPointer - bPointer

                const comp = compareOver(s, aPointer, bPointer, Math.min(aLen, bLen))
                if (comp == 0) {
                    return aLen - bLen
                } else {
                    return comp
                }
            default:
                throw new Error('bug')
        }
    }
}
