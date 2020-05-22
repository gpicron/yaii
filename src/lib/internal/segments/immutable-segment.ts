import {Term, TermExp} from "../query-ir/term-exp"
import {Doc, DocId, FieldName, FieldValue, FieldValues, ResultItem} from "../../api/base"
import {as, AsyncIterableX, concat, from, isEmpty, sum} from "ix/asynciterable"

import LevelUp, * as lu from "levelup"
import LevelDOWN, * as ld from "leveldown"
import {BitmapDocidAsyncIterable} from "../datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {BaseSegment} from "./segment"
import {MutableSegment} from "./mutable-segment"
import {
    assertUnreachable,
    ExtFieldsIndexConfig,
    ExtIndexConfig,
    ICompareFunction,
    opinionatedCompare,
    reverseCompareFunction,
    waitForImmediate
} from "../utils"
import {FieldConfigFlag, FieldConfigFlagSet} from "../../api/config"
import {DocPackedArray, FieldTag, generateDecoder, RecordSchema} from "../datastructs/doc-packed-array"
import {PutBatch} from "abstract-leveldown"
import * as op from "ix/asynciterable/operators"
import {convertU32ToLex64, TermPrefix} from "../query-ir/query-ir"
import {
    ColumnStore,
    ColumnStoreBuilder,
    DenseFixLenString,
    DenseVarLenString,
    NumberColumnStore,
    NumberColumnStoreBuilder,
    SparseVarLenString,
    StringColumnStoreBuilder,
    ValueStoreType
} from "../datastructs/stores/base"
import {RoaringBitmap32} from "roaring"
import ByteBuffer from "bytebuffer"
import {SingletonDocidAsyncIterable} from "../datastructs/docid-async-iterable/singleton-docid-async-iterable"
import {MemoryLimitedLRUCache} from "../datastructs/lru-cache"
import {SortClause, SortDirection} from "../../api/query"
import {ProjectionsAndComparator} from "../../../base-inverted-index"
import {DocIdIterable} from "../datastructs/docid-async-iterable/base"
import {EMPTY_MAP} from "../datastructs/docid-async-iterable/range-docid-async-iterable"


enum InternalKeys {
    CONFIG = "_£_CONFIG",
    SOURCE_STORE_PREFIX = "$",
    VALUE_STORE_PREFIX_DATA = "£D",
    VALUE_STORE_DEFAULT_POINTERS = "££P",
    VALUE_STORE_DEFAULT_BUFFER = "££D",
    DELETED = "£££",
}


export type ImmutableSegmentFieldConfigV1 = {
    flags: FieldConfigFlagSet
    valueStoreType?: ValueStoreType

}

interface ImmutableSegmentConfig {
    version: number

}

interface ImmutableSegmentConfigV1 extends ImmutableSegmentConfig {
    version: 1
    sourceSchema?: RecordSchema
    fields: Record<FieldName, ImmutableSegmentFieldConfigV1>
    defaultValueStoreSchema?: RecordSchema
}



export class ImmutableSegment extends BaseSegment {
    private static FIELD_TERM_SEPARATOR = "\x1E"
    private static NEXT_FIELD_TERM_SEPARATOR_BOUNDARY = "\x1F"

    readonly next: number
    readonly rangeSize: number
    private db?: lu.LevelUp
    private segmentConfig?: ImmutableSegmentConfigV1
    private sourceDecoder?: (buffer: Buffer) =>  Doc

    private _deleted: BitmapDocidAsyncIterable

    private cache  = new MemoryLimitedLRUCache(Number.MAX_SAFE_INTEGER)

    private constructor(id: number, fieldsIndexConfig: ExtFieldsIndexConfig, indexConfig: ExtIndexConfig, from: number, next: number, deleted: BitmapDocidAsyncIterable) {

        super(id, Object.assign({},fieldsIndexConfig), Object.assign({},indexConfig), from)
        this.next = next;
        this.rangeSize = next - from
        this._deleted = deleted

    }

    static async fromMutable(mutableSegment: MutableSegment) {
        const deleted = mutableSegment.deleted()
        const r =  new ImmutableSegment(mutableSegment.id, mutableSegment.fieldsIndexConfig, mutableSegment.indexConfig, mutableSegment.from, mutableSegment.next, deleted.readOnly())

        const levelDown = LevelDOWN(`${r.indexConfig.storePath}/segment-${r.id}`)
        const db = LevelUp(levelDown, {
            compression: false,
            errorIfExists: true,
            createIfMissing: true,
            writeBufferSize: 32 * 1024 * 1024
        } as ld.LevelDownOpenOptions)

        await db.open()

        const permanentFieldsConfig = Object.entries(mutableSegment.fieldsIndexConfig).reduce(
            (acc, e) => {
                acc[e[0]] = {
                    flags: e[1].flags
                }
                return acc
            },
            {} as Record<FieldName, ImmutableSegmentFieldConfigV1>
        )

        r.segmentConfig = {
            version: 1,
            sourceSchema : mutableSegment.docSourceStore ? mutableSegment.docSourceStore.rootSchema: undefined,
            fields: permanentFieldsConfig
        }

        // store docs
        if (mutableSegment.docSourceStore) {
            const buffer = mutableSegment.docSourceStore
            buffer.store().flip().compact()
            const a =  await sum(as(mutableSegment.docSourceStore.iterateBuffers()).pipe(
                op.map((buffer: Buffer, index: number) => ({
                    type: "put",
                    key: InternalKeys.SOURCE_STORE_PREFIX + convertU32ToLex64(index),
                    value: buffer
                } as PutBatch)),
                op.buffer(1000),
                op.tap(async () => waitForImmediate()),
                op.map( async bat => {
                    return db.batch(bat ).then(() => bat.length)
                })
            ))

            console.log("written doc sources:", a)
        }

        // which fields in columns ?
        const columnStoreBuilders = new  Map<FieldName, ColumnStoreBuilder<FieldValue>>()
        const remainingStoredFields = DocPackedArray.createNew()

        const fieldValueSchema = mutableSegment.perFieldValue.rootSchema
        const notSpecialized = new Array<FieldName>()
        for (const [fieldName,configs]  of fieldValueSchema.fields.entries()) {
            if (configs.length == 1) {
                const config = configs[0]
                switch (config.kind) {
                    case FieldTag.Numeric:
                        columnStoreBuilders.set(fieldName, new NumberColumnStoreBuilder(config.getMinimumBufferFixedLengthDataType()))
                        break;
                    case FieldTag.String:
                        columnStoreBuilders.set(fieldName, new StringColumnStoreBuilder())
                        break;
                    case FieldTag.Boolean:
                    case FieldTag.Child:
                    case FieldTag.MixedArray:
                    case FieldTag.StringArray:
                    case FieldTag.NumericArray:
                    case FieldTag.BufferValue:
                    case FieldTag.ChildArray:
                        notSpecialized.push(fieldName)
                        break;
                    default:
                        assertUnreachable(config.kind)

                }
            }
        }

        const orderOptimizedColumns = new  Map<FieldName, Array<FieldValues | FieldValue>>()
        for (const field in mutableSegment.fieldsIndexConfig) {
            const config =  mutableSegment.fieldsIndexConfig[field]
            if ((config.flags & FieldConfigFlag.SORT_OPTIMIZED) != 0) {
                orderOptimizedColumns.set(field, [])
            }
        }
        // traverse and build columns stores
        let index = 0
        for await (const fieldSet of mutableSegment.perFieldValue.iterateObjects()) {
            for (const [field, array] of orderOptimizedColumns) {
                array.push(fieldSet[field] as FieldValues | FieldValue)
            }

            for (const [field, builder] of columnStoreBuilders.entries()) {
                const v = fieldSet[field] as FieldValue
                if (v !== undefined) {
                    builder.add(index, v)
                    delete fieldSet[field]
                }
            }

            remainingStoredFields.add(fieldSet)
            index++
        }


        for (const [field, array] of orderOptimizedColumns) {
            const ranks = getRank(array)

            const max = ranks.reduce((previousValue, currentValue) => currentValue > previousValue ? currentValue : previousValue, 0)
            let storeType
            if (max < 256) {
                storeType = ValueStoreType.Uint8
            } else if (max < 65536) {
                storeType = ValueStoreType.Uint16
            } else {
                storeType = ValueStoreType.Uint32
            }

            const builder = new NumberColumnStoreBuilder(storeType)
            for (let i = 0; i < index; i++) {
                builder.add(i, ranks[i])
            }

            const column = builder.build()

            r.segmentConfig.fields[field].valueStoreType = column.storeType
            await db.put(InternalKeys.VALUE_STORE_PREFIX_DATA + field + '.rank', column.serialize())
        }



        for (const [field, builder] of columnStoreBuilders.entries()) {
            const column = builder.build()
            r.segmentConfig.fields[field].valueStoreType = column.storeType

            await db.put(InternalKeys.VALUE_STORE_PREFIX_DATA + field, column.serialize())
        }

        for (const field of notSpecialized) {
            r.segmentConfig.fields[field].valueStoreType = ValueStoreType.DEFAULT
        }

        const deletedBitmap = deleted.bitmap
        deletedBitmap.runOptimize()

        await db.batch([
            { type:"put", key:InternalKeys.VALUE_STORE_DEFAULT_POINTERS , value: remainingStoredFields.pointers.serialize() },
            { type:"put", key:InternalKeys.VALUE_STORE_DEFAULT_BUFFER, value: remainingStoredFields.store().flip().compact().buffer },
            { type:"put", key:InternalKeys.DELETED, value: deletedBitmap.serialize() }
        ])

        r.segmentConfig.defaultValueStoreSchema = remainingStoredFields.rootSchema


        // store the posting lists
        const totalPostingLists = await sum(from(mutableSegment.perFieldMap.entries()).pipe(
            op.orderBy(([field, _termsMap]) => field),
            op.flatMap(([field, termsMap]: [FieldName, Map<Term, RoaringBitmap32 | number>]) => {
                return from(termsMap.entries()).pipe(
                    op.orderBy(([term, _postingList]) => term),
                    op.map(([term, postingList]: [Term, RoaringBitmap32 | number])  => {
                        let data
                        if (typeof postingList === 'number') {
                            data = Buffer.allocUnsafe(4)
                            data.writeUInt32LE(postingList)
                        } else {
                            postingList.runOptimize()
                            data = postingList.serialize()
                        }

                        return { type:"put", key:field + ImmutableSegment.FIELD_TERM_SEPARATOR + term, value: data } as PutBatch
                    })
                )
            }),
            op.buffer(1000),
            op.tap(async () => waitForImmediate()),
            op.map( async bat => {
                return db.batch(bat).then(() => bat.length)
            })

        ))
        console.log("written posting lists:", totalPostingLists)

        await db.batch([
            { type:"put", key:InternalKeys.CONFIG, value:JSON.stringify(r.segmentConfig)}
        ], { sync:true } as ld.LevelDownBatchOptions)


        await db.close()

        r.db = LevelUp(levelDown, {
            compression: false,
            errorIfExists: false,
            createIfMissing: false,
            cacheSize: 32 * 1024 * 1024
        } as ld.LevelDownOpenOptions)

        await r.db.open()

        const sourceDecodingFunction = generateDecoder(r.segmentConfig.sourceSchema as RecordSchema).function

        r.sourceDecoder = (buffer: Buffer) => {
            const bb = ByteBuffer.wrap(buffer)
            return sourceDecodingFunction(0, bb)
        }

        return r
    }

    async get(field: FieldName, term: Term): Promise<DocIdIterable> {

        const key = field + ImmutableSegment.FIELD_TERM_SEPARATOR + term

        return this.cache.get(this.id + key, async () => {
            try {
                const buffer = await this.db?.get(key, { asBuffer: true })
                if (buffer.length === 4){
                    return new SingletonDocidAsyncIterable(buffer.readUInt32LE())
                } else {
                    return new BitmapDocidAsyncIterable(false, RoaringBitmap32.deserialize(buffer))
                }
            } catch (e) {
                if (e.name === 'NotFoundError') {
                    return EMPTY_MAP
                }

                throw e
            }
        }) as unknown as DocIdIterable
    }

    async mayMatch(term: TermExp): Promise<boolean> {
        const key = term.field + ImmutableSegment.FIELD_TERM_SEPARATOR + term.term

        const cached = this.cache.get(this.id + key) as unknown as DocIdIterable | undefined

        if (cached && cached.cost == 0) return false

        return true

        const keyRead = this.db?.createKeyStream({
            gte: new Buffer(key, 'utf8'),
            lte: new Buffer(key, 'utf8')
        }) as NodeJS.ReadableStream

        return isEmpty(from(keyRead)).then(empty => {
            if (empty) this.cache.put(this.id + key, EMPTY_MAP)
            return !empty
        })
    }


    terms(field: FieldName): AsyncIterableX<Term> {
        const prefix = field + ImmutableSegment.FIELD_TERM_SEPARATOR
        const prefixLen = prefix.length
        const prefixString = prefix + TermPrefix.STRING
        const prefixNumber = prefix + TermPrefix.NUMBER_L0
        const keyTrue = prefix + TermPrefix.BOOLEAN_TRUE
        const keyFalse = prefix + TermPrefix.BOOLEAN_FALSE

        const stringTerms = this.db?.createKeyStream({
            gte: new Buffer(prefixString, 'utf8'),
            lt: new Buffer(prefixNumber, 'utf8'),
            keyAsBuffer: false
        }) as NodeJS.ReadableStream

        // TODO number terms (must combine L0 and L1)

        const booleanTerms = this.db?.createKeyStream({
            gte: new Buffer(keyTrue, 'utf8'),
            lte: new Buffer(keyFalse, 'utf8'),
            keyAsBuffer: false
        }) as NodeJS.ReadableStream

        return concat(
            from(stringTerms).pipe(
                op.map((key) => (key as string).substring(prefixLen))
            ),
            from(booleanTerms).pipe(
                op.tap(console.log),
                op.map((key) => (key as string).substring(prefixLen))
            )
        )

    }

    addToDeleted(docId: DocId): void {
        throw new Error(`not yet implemented ${docId}`)
    }

    deleted(): BitmapDocidAsyncIterable {
        return this._deleted.readOnly();
    }

    addProjections<T extends Doc>(source: AsyncIterable<ResultItem<T>>, projection?: string[]): AsyncIterableX<ResultItem<T>> {
        if (projection == undefined) {
            let generator
            if (this.sourceDecoder) {
                const sourceDecoder = this.sourceDecoder
                const db = this.db
                generator = async function* () {
                    for await (const r of source) {
                        try {
                            r._source = sourceDecoder(await db?.get(InternalKeys.SOURCE_STORE_PREFIX + convertU32ToLex64(r._id))) as T

                            yield r
                        } catch (e) {
                            console.log('fail to load:', r._id)
                            throw e
                        }
                    }
                }
                return from(generator())
            } else {
                return as(source);
            }

        } else {
            const segmentConfig = this.segmentConfig
            const cache = this.cache
            const id = this.id
            const db = this.db

            const generator = async function* () {
                const fromDefault = new Array<FieldName>()
                const fromColumnStore = new Map<FieldName, ColumnStore<FieldValue>>()


                for (const p of projection) {

                    const storeType = segmentConfig?.fields[p]?.valueStoreType
                    if (storeType === ValueStoreType.DEFAULT) {
                        fromDefault.push(p)
                    } else if (storeType !== undefined) {
                        const key = "$" + p
                        const columnStore = await cache.get(id + key, async () => {
                            const serialized = await db?.get(InternalKeys.VALUE_STORE_PREFIX_DATA + p, {asBuffer:true})

                            switch (storeType) {
                                case ValueStoreType.Int8:
                                case ValueStoreType.Uint8:
                                case ValueStoreType.Int16:
                                case ValueStoreType.Uint16:
                                case ValueStoreType.Int32:
                                case ValueStoreType.Uint32:
                                case ValueStoreType.Int64:
                                case ValueStoreType.Uint64:
                                case ValueStoreType.Float64:
                                    return NumberColumnStore.load(serialized, storeType)
                                case ValueStoreType.StringDenseFixLen:
                                    return DenseFixLenString.load(serialized)
                                case ValueStoreType.StringDenseVarLen:
                                    return DenseVarLenString.load(serialized)
                                case ValueStoreType.StringSparseVarLen:
                                    return SparseVarLenString.load(serialized)
                                default:
                                    throw assertUnreachable(storeType)}

                        }) as ColumnStore<FieldValue>

                        fromColumnStore.set(p, columnStore)
                    }
                }

                let defaultStore

                if (fromDefault.length > 0) {
                    const key = "$"
                    defaultStore = await cache.get(id + key, async () => {
                        const serialized = await db?.get(InternalKeys.VALUE_STORE_DEFAULT_POINTERS, {asBuffer:true})
                        const pointers = RoaringBitmap32.deserialize(serialized)
                        const buf = ByteBuffer.wrap(await db?.get(InternalKeys.VALUE_STORE_DEFAULT_BUFFER, {asBuffer:true}))

                        return DocPackedArray.load(() => buf, pointers, segmentConfig?.defaultValueStoreSchema as RecordSchema)
                    }) as DocPackedArray

                }


                for await (const r of source) {
                    for (const [f, sto] of fromColumnStore.entries()) {
                        r[f] = sto.get(r._id)
                    }

                    if (defaultStore) {
                        const colGroup = defaultStore.get(r._id)

                        if (colGroup) {
                            for (const f of fromDefault) {
                                const colGroupElement = colGroup[f]

                                if (colGroupElement) {
                                    r[f] = colGroupElement as FieldValue | FieldValues | Buffer
                                }
                            }
                        }
                    }

                    yield r
                }

            }
            return from(generator())
        }

    }

    buildComparatorAndProjections(sortClauses: Array<SortClause>): ProjectionsAndComparator {

        const sortProjection = new Array<FieldName>()

        const comparators = new Array<ICompareFunction<ResultItem<Doc>>>()
        for (const clause of sortClauses) {
            let field: FieldName
            let dir

            if (typeof clause === 'string') {
                field = clause
                dir = SortDirection.ASCENDING
            } else {
                field = clause.field
                dir = clause.dir == SortDirection.DESCENDING ? SortDirection.DESCENDING : SortDirection.ASCENDING
            }

            const config = this.fieldsIndexConfig[field]

            if (!config || !(config.flags & FieldConfigFlag.STORED || config.flags & FieldConfigFlag.SORT_OPTIMIZED)) {
                throw new Error(
                    `Sorting not supported for field that is not STORED or SORT_OPTIMIZED : ${field}`
                )
            }

            let fieldComparator: ICompareFunction<ResultItem<Doc>>
            if (config.flags & FieldConfigFlag.SORT_OPTIMIZED) {
                const rankField = field + '.rank'
                sortProjection.push(rankField)
                fieldComparator = (a, b) => (a[rankField] as number) - (b[rankField] as number)
            } else {
                sortProjection.push(field)
                fieldComparator = (a, b) => {
                    const aElement = a[field]
                    const aVal = Array.isArray(aElement)
                        ? aElement[0]
                        : (aElement as FieldValue | undefined | Buffer)
                    const bElement = b[field]
                    const bVal = Array.isArray(bElement)
                        ? bElement[0]
                        : (bElement as FieldValue | undefined | Buffer)

                    return opinionatedCompare(aVal, bVal)
                }
            }



            if (dir === SortDirection.DESCENDING) {
                fieldComparator = reverseCompareFunction(fieldComparator)
            }

            comparators.push(fieldComparator)
        }

        const compare = (a: ResultItem<Doc>, b: ResultItem<Doc>) => {
            for (const comp of comparators) {
                const v = comp(a, b);
                if (v != 0) return -v
            }
            return 0
        }

        return {
            projections: sortProjection,
            comparator: compare
        }

    }
}

function getRank(array: (FieldValue | undefined | Buffer | FieldValues)[]): number[] {
    return array
        .map((x, i: number) => ({v:Array.isArray(x) ? x[0]: x, i:i}))
        .sort((a, b) => opinionatedCompare(a.v, b.v))
        .reduce((a: Array<number>, x, i, s) => (a[x.i] =
            i > 0 && opinionatedCompare(s[i - 1].v, x.v) === 0 ? a[s[i - 1].i] : i + 1, a), new Array<number>());
}
