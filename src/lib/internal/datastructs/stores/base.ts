import ByteBuffer = require('bytebuffer')
import Long = require('long')
import {RoaringBitmap32} from "roaring"
import {FieldValue} from "../../../api/base"
import {ValueWithMemoryEstimation} from "../lru-cache"
import {assertUnreachable} from "../../utils"

export type ByteBufferAccess = () => ByteBuffer

export enum ValueStoreType {
    DEFAULT = "DEFAULT",
    Uint8 = "Uint8",
    Int8 = "Int8",
    Uint16 = "Uint16",
    Int16 = "Int16",
    Uint32 = "Uint32",
    Int32 = "Int32",
    Uint64 = "Uint64",
    Int64 = "Int64",
    Float64 = "Float64",

    StringDenseFixLen = "StringDenseFixLen",
    StringDenseVarLen = "StringDenseVarLen",
    StringSparseVarLen = "StringSparseVarLen",


}

export interface ColumnStoreBuilder<T extends FieldValue> {
    add(index: number, value: T): void
    build(): ColumnStore<T>
}

export interface ColumnStore<T extends FieldValue> extends ValueWithMemoryEstimation{
    readonly storeType: ValueStoreType
    get(index: number): T | undefined

    serialize(): Buffer
}

export class NumberColumnStore implements ColumnStore<number> {
    readonly storeType: ValueStoreType
    readonly buffer: Buffer
    readonly present?: RoaringBitmap32
    private readonly method: (buffer: Buffer, offset: number) => number
    private readonly bytesPerValue: number


    constructor(buffer: Buffer, type: ValueStoreType, present?: RoaringBitmap32) {
        this.storeType = type
        this.buffer = buffer
        this.present = present

        switch (type) {
            case ValueStoreType.Int8:
                this.method = NumberColumnStore.readInt8
                this.bytesPerValue = 1
                break;
            case ValueStoreType.Uint8:
                this.method = NumberColumnStore.readUInt8
                this.bytesPerValue = 1
                break;
            case ValueStoreType.Int16:
                this.method = NumberColumnStore.readInt16
                this.bytesPerValue = 2
                break;
            case ValueStoreType.Uint16:
                this.method = NumberColumnStore.readUInt16
                this.bytesPerValue = 2
                break;
            case ValueStoreType.Int32:
                this.method = NumberColumnStore.readInt32
                this.bytesPerValue = 4
                break;
            case ValueStoreType.Uint32:
                this.method = NumberColumnStore.readUInt32
                this.bytesPerValue = 4
                break;
            case ValueStoreType.Int64:
                this.method = NumberColumnStore.readInt64
                this.bytesPerValue = 8
                break;
            case ValueStoreType.Uint64:
                this.method = NumberColumnStore.readUInt64
                this.bytesPerValue = 8
                break;
            case ValueStoreType.Float64:
                this.method = NumberColumnStore.readFloat64
                this.bytesPerValue = 8
                break;
            case ValueStoreType.DEFAULT:
            case ValueStoreType.StringDenseFixLen:
            case ValueStoreType.StringDenseVarLen:
            case ValueStoreType.StringSparseVarLen:
                throw new Error("Bug")
            default:
                throw assertUnreachable(type)
        }

    }

    get(index: number): number | undefined {
        if (this.present) {
            if (this.present.has(index)) {
                return this.method(this.buffer, this.present.rank(index) * this.bytesPerValue || 0);
            } else {
                return undefined
            }
        } else {
            return this.method(this.buffer, index * this.bytesPerValue);
        }
    }

    get sizeInMemory() {
        return this.buffer.length +  (this.present ? this.present.getSerializationSizeInBytes() : 0)
    }

    private static readInt64(buffer: Buffer, offset: number): number {
        const lo = buffer.readUInt32LE(offset),
            hi = buffer.readUInt32LE(offset+4);
        return new Long(lo, hi, false).toNumber();
    };


    private static readUInt64(buffer: Buffer, offset: number): number {
        const lo = buffer.readUInt32LE(offset),
            hi = buffer.readUInt32LE(offset+4);
        return new Long(lo, hi, true).toNumber();
    };

    private static readInt32(buffer: Buffer, offset: number): number {
        return buffer.readInt32LE(offset);
    };

    private static readUInt32(buffer: Buffer, offset: number): number {
        return buffer.readUInt32LE(offset);
    };

    private static readInt16(buffer: Buffer, offset: number): number {
        return buffer.readInt16LE(offset);
    };

    private static readUInt16(buffer: Buffer, offset: number): number {
        return buffer.readUInt16LE(offset);
    };

    private static readInt8(buffer: Buffer, offset: number): number {
        return buffer.readInt8(offset);
    };

    private static readUInt8(buffer: Buffer, offset: number): number {
        return buffer.readUInt8(offset);
    };

    private static readFloat64(buffer: Buffer, offset: number): number {
        return buffer.readFloatLE(offset);
    };

    serialize(): Buffer {
        let buf
        if (this.present) {
            const serialized = this.present.serialize()
            buf = ByteBuffer.allocate(4 + serialized.length + this.buffer.length)
            buf.writeUint32(serialized.length)
            buf.append(serialized)
            buf.append(this.buffer)
        } else {
            buf = ByteBuffer.allocate(4 + this.buffer.length)
            buf.writeUint32(0)
            buf.append(this.buffer)
        }

        return buf.flip().toBuffer(false);
    }

    static load(serialized: Buffer, storeType: ValueStoreType.Int8 | ValueStoreType.Uint8 | ValueStoreType.Int16 | ValueStoreType.Uint16 | ValueStoreType.Int32 | ValueStoreType.Uint32 | ValueStoreType.Int64 | ValueStoreType.Uint64 | ValueStoreType.Float64) {
        const bb = ByteBuffer.wrap(serialized)
        const lenPres = bb.readUint32()
        if (lenPres === 0) {
            return new NumberColumnStore(bb.buffer.slice(4), storeType)
        } else {
            const present= RoaringBitmap32.deserialize(bb.slice(4, 4 + lenPres).toBuffer(true))
            const dataBuffer = bb.slice(4 + lenPres).toBuffer(true)
            return new NumberColumnStore(dataBuffer, storeType, present)
        }
    }
}

export class NumberColumnStoreBuilder implements ColumnStoreBuilder<number> {
    private buffer = ByteBuffer.allocate(1024, true);
    private present = new RoaringBitmap32()
    private last = -1;
    readonly storeType: ValueStoreType
    private writeMethod: (value: number) => ByteBuffer

    constructor(storeType: ValueStoreType) {
        this.storeType = storeType

        switch (this.storeType) {
            case ValueStoreType.Uint8:
                this.writeMethod = ByteBuffer.prototype.writeUint8
                break;
            case ValueStoreType.Uint16:
                this.writeMethod = ByteBuffer.prototype.writeUint16
                break;
            case ValueStoreType.Uint32:
                this.writeMethod = ByteBuffer.prototype.writeUint32
                break;
            case ValueStoreType.Uint64:
                this.writeMethod = ByteBuffer.prototype.writeUint64
                break;
            case ValueStoreType.Float64:
                this.writeMethod = ByteBuffer.prototype.writeFloat64
                break;
            case ValueStoreType.Int8:
                this.writeMethod = ByteBuffer.prototype.writeInt32
                break;
            case ValueStoreType.Int16:
                this.writeMethod = ByteBuffer.prototype.writeInt16
                break;
            case ValueStoreType.Int32:
                this.writeMethod = ByteBuffer.prototype.writeInt32
                break;
            case ValueStoreType.Int64:
                this.writeMethod = ByteBuffer.prototype.writeInt64
                break;
            case ValueStoreType.DEFAULT:
            case ValueStoreType.StringSparseVarLen:
            case ValueStoreType.StringDenseVarLen:
            case ValueStoreType.StringDenseFixLen:
                throw new Error(`${this.storeType} is not a numeric type` )
            default:
                throw assertUnreachable(this.storeType)

        }
    }

    add(index: number, value: number): void {
        if (index < this.last) throw new Error("items must be added sequentially")
        this.last = index
        this.present.add(index)
        this.writeMethod.call(this.buffer, value)
    }

    build(): ColumnStore<number> {
        const byteBuffer = this.buffer.flip().compact()
        const present = this.present
        if (present.maximum() == present.size - 1) {
            return new NumberColumnStore(byteBuffer.toBuffer(false), this.storeType, undefined);
        } else {
            return new NumberColumnStore(byteBuffer.toBuffer(false), this.storeType, present);
        }
    }

}

export class DenseFixLenString implements ColumnStore<string> {
    readonly storeType = ValueStoreType.StringDenseFixLen
    readonly buffer: Buffer
    readonly len: number


    constructor(buffer: Buffer, len: number) {
        this.buffer = buffer
        this.len = len
    }

    get(index: number): string | undefined {
        const start = index * this.len
        const end = (index + 1) * this.len
        return this.buffer.toString("utf8", start, end);
    }

    get sizeInMemory() {
        return this.buffer.length
    }

    serialize(): Buffer {
        const buf = ByteBuffer.allocate(4 + this.buffer.length)
        buf.writeUint32(this.len)
        buf.append(this.buffer)
        return buf.flip().toBuffer(false);
    }

    static load(serialized: Buffer) {
        const buf = ByteBuffer.wrap(serialized)
        const len = buf.readUint32()
        return new DenseFixLenString(serialized.slice(4), len)
    }
}

export class DenseVarLenString implements ColumnStore<string> {
    readonly storeType = ValueStoreType.StringDenseVarLen
    readonly buffer: Buffer
    readonly pointers: RoaringBitmap32


    constructor(buffer: Buffer, pointers: RoaringBitmap32) {
        this.buffer = buffer
        this.pointers = pointers
    }

    get(index: number): string | undefined {
        const start = this.pointers.select(index - 1) || 0
        const end = this.pointers.select(index)
        if (end == start) return ''

        return this.buffer.toString("utf8", start, end);
    }

    get sizeInMemory() {
        return this.buffer.length + this.pointers.getSerializationSizeInBytes()
    }
    serialize(): Buffer {
        const serPointers = this.pointers.serialize()
        const buf = ByteBuffer.allocate(4 + serPointers.length + this.buffer.length)
        buf.writeUint32(serPointers.length)
        buf.append(serPointers)
        buf.append(this.buffer)
        return buf.flip().toBuffer(false);
    }

    static load(serialized: Buffer) {
        const bb = ByteBuffer.wrap(serialized)
        const lenPointers = bb.readUint32()
        const pointers= RoaringBitmap32.deserialize(bb.slice(4, 4 + lenPointers).toBuffer(true))
        const dataBuffer = bb.slice(4 + lenPointers).toBuffer(true)

        return new DenseVarLenString(dataBuffer,pointers)
    }

}

export class SparseVarLenString implements ColumnStore<string> {
    readonly storeType = ValueStoreType.StringSparseVarLen
    readonly buffer: Buffer
    readonly pointers: RoaringBitmap32
    readonly present: RoaringBitmap32


    constructor(buffer: Buffer, pointers: RoaringBitmap32, present: RoaringBitmap32) {
        this.buffer = buffer
        this.pointers = pointers
        this.present = present
    }

    get(index: number): string | undefined {
        if (!this.present.has(index)) return undefined

        const rank = this.present.rank(index)

        const start = this.pointers.select(rank - 1) || 0
        const end = this.pointers.select(rank)
        if (end == start) return ''

        return this.buffer.toString("utf8", start, end);
    }

    get sizeInMemory() {
        return this.buffer.length + this.pointers.getSerializationSizeInBytes() + this.present.getSerializationSizeInBytes()
    }
    serialize(): Buffer {
        const serPointers = this.pointers.serialize()
        const serPresent = this.present.serialize()
        const buf = ByteBuffer.allocate(8 + serPointers.length + serPresent.length + this.buffer.length)
        buf.writeUint32(serPointers.length)
        buf.append(serPointers)
        buf.writeUint32(serPresent.length)
        buf.append(serPresent)
        buf.append(this.buffer)
        return buf.flip().toBuffer(false);
    }

    static load(serialized: Buffer) {
        const bb = ByteBuffer.wrap(serialized)
        const lenPointers = bb.readUint32()
        const pointers= RoaringBitmap32.deserialize(bb.slice(4, 4 + lenPointers).toBuffer(true))
        const lenPres = bb.readUint32(4 + lenPointers)
        const present= RoaringBitmap32.deserialize(bb.slice(8 + lenPointers, 8 + lenPres + lenPointers).toBuffer(true))

        const dataBuffer = bb.slice(8 + lenPres + lenPointers).toBuffer(true)

        return new SparseVarLenString(dataBuffer, pointers, present)
    }
}


export class StringColumnStoreBuilder implements ColumnStoreBuilder<string> {
    private buffer = ByteBuffer.allocate(1024, true);
    private present = new RoaringBitmap32()
    private pointers = new RoaringBitmap32()
    private last = -1;
    private len?: number

    constructor() {
        // do nothing
    }

    add(index: number, value: string): void {
        if (index < this.last) throw new Error("items must be added sequentially")
        this.last = index
        const currentPointer = this.buffer.offset

        this.present.add(index)

        this.buffer.writeUTF8String(value)
        const nextPointer = this.buffer.offset
        this.pointers.add(nextPointer)
        this.buffer.limit = this.buffer.capacity()

        if (this.len === undefined) {
            this.len = nextPointer - currentPointer
        } else if (this.len >= 0 && this.len !== nextPointer - currentPointer) {
            this.len = -1
        }

    }

    build(): ColumnStore<string> {
        const byteBuffer = this.buffer.flip().compact().toBuffer(false)
        const present = this.present
        if (present.maximum() == present.size - 1) {
            if (this.len && this.len >= 0) {
                return new DenseFixLenString(byteBuffer, this.len)
            } else {
                return new DenseVarLenString(byteBuffer, this.pointers)
            }
        } else {
            return new SparseVarLenString(byteBuffer, this.pointers, present);
        }
    }

}
