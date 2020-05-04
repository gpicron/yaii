import { Doc, FieldConfigFlag, FieldName, FieldStorableValue, FieldValue, FieldValues, ResultItem } from '../yaii-types'
import { AsyncIterableX, count, from } from 'ix/asynciterable'
import { RoaringBitmap32 } from 'roaring'
import * as op from 'ix/asynciterable/operators'
import { DocId, ExtFieldConfig, ExtFieldsIndexConfig, ExtIndexConfig, ICompareFunction } from './utils'
import { BitmapAsyncIterable, DocIdAsyncIterable, SingletonDocIdAsyncIterable } from './bitmap'
import { IndexableDoc } from '../index'
import { DocPackedArray } from './doc-packed-array'
import { INTERNAL_FIELDS, stringToTerm } from './query-ir'
import { SortFieldPackedArray } from './sort_field_packed_array'

type B64Token = string

export class IndexSegment {
    private fieldsIndexConfig: ExtFieldsIndexConfig
    private indexConfig: ExtIndexConfig
    private perFieldMap: Map<FieldName, Map<B64Token, RoaringBitmap32 | number>> = new Map()
    private perFieldValue = new DocPackedArray()
    private docSourceStore: DocPackedArray | undefined
    private perSortedFieldValue: Map<FieldName, SortFieldPackedArray> = new Map()
    from: number
    next: number

    private hasStoredFields: boolean = false

    constructor(fieldsIndexConfig: ExtFieldsIndexConfig, indexConfig: ExtIndexConfig, from?: number) {
        this.from = from || 0
        this.next = from || 0

        this.fieldsIndexConfig = fieldsIndexConfig
        this.indexConfig = indexConfig

        if (this.indexConfig.storeSourceDoc) {
            this.docSourceStore = new DocPackedArray()
        }

        for (const [field, fConfig] of Object.entries(fieldsIndexConfig)) {
            this.setupStorageForField(field, fConfig)
            this.perFieldMap.set(INTERNAL_FIELDS.FIELDS, new Map())
        }
    }

    private setupStorageForField(field: FieldName, fConfig: ExtFieldConfig) {
        if (fConfig.flags & FieldConfigFlag.SEARCHABLE) {
            this.perFieldMap.set(field, new Map())
        }

        if (fConfig.flags & FieldConfigFlag.STORED) {
            this.hasStoredFields = true
        }

        if (fConfig.flags & FieldConfigFlag.SORT_OPTIMIZED) {
            this.perSortedFieldValue.set(field, new SortFieldPackedArray())
        }
    }

    async add(docs: AsyncIterableX<IndexableDoc>): Promise<number> {
        const start = this.next

        const insertDocument = async (doc: IndexableDoc, index: number) => {
            index += start

            const source = doc[INTERNAL_FIELDS.SOURCE]
            if (source && this.docSourceStore) {
                this.docSourceStore.add(source)
            }

            const nonEmptyFields = new Map<string, boolean>()
            const storedFields: Doc = {}

            for (const [fieldName, fieldValue] of Object.entries(doc)) {
                if (typeof fieldValue !== 'undefined') {
                    let conf = this.fieldsIndexConfig[fieldName]

                    if (!conf && this.indexConfig.defaultFieldConfig) {
                        conf = this.indexConfig.defaultFieldConfig

                        this.setupStorageForField(fieldName, conf)

                        this.fieldsIndexConfig[fieldName] = conf
                    }

                    if (conf) {
                        // update value columns
                        if (conf.flags & FieldConfigFlag.STORED) {
                            storedFields[fieldName] = fieldValue

                            nonEmptyFields.set(stringToTerm(fieldName).toString('base64'), true)
                        }

                        // update bitmaps
                        if (conf.flags & FieldConfigFlag.SEARCHABLE && !Buffer.isBuffer(fieldValue)) {
                            const tokens = conf.tokenizer(fieldValue)

                            if (tokens.length > 0) {
                                const mapTree = this.perFieldMap.get(fieldName) as Map<B64Token, RoaringBitmap32 | number>
                                for (const token of tokens) {
                                    const key = token.toString('base64')
                                    const map = mapTree.get(key)
                                    if (map == undefined) {
                                        mapTree.set(key, index)
                                    } else if (typeof map === 'number') {
                                        mapTree.set(key, RoaringBitmap32.from([map, index]))
                                    } else {
                                        map.add(index)
                                    }
                                }

                                nonEmptyFields.set(stringToTerm(fieldName).toString('base64'), true)
                            }
                        }

                        // update sort fields
                        if (conf.flags & FieldConfigFlag.SORT_OPTIMIZED && !Buffer.isBuffer(fieldValue)) {
                            const val = Array.isArray(fieldValue) ? fieldValue[0] : fieldValue

                            const array = this.perSortedFieldValue.get(fieldName) as SortFieldPackedArray
                            array.add(val)
                        }
                    }
                }
            }
            // index non empty fields list
            if (nonEmptyFields.size > 0) {
                const mapTree = this.perFieldMap.get(INTERNAL_FIELDS.FIELDS) as Map<B64Token, RoaringBitmap32>

                for (const key of nonEmptyFields.keys()) {
                    const map = mapTree.get(key)
                    if (map == undefined) {
                        mapTree.set(key, RoaringBitmap32.from([index]))
                    } else {
                        map.add(index)
                    }
                }
            }

            if (this.hasStoredFields && storedFields) {
                this.perFieldValue.add(storedFields)
            }
        }

        const total = await count(docs.pipe(op.map(insertDocument)))
        this.next += total

        return total
    }

    get size() {
        return this.next - this.from
    }

    project<T extends Doc>(docIds: AsyncIterable<number>, projection: Array<FieldName>): AsyncIterableX<ResultItem<T>> {
        const fromIndex = this.from

        if (projection.length == 1 && projection[0] === INTERNAL_FIELDS.SOURCE) {
            const store = this.docSourceStore
            let generator
            if (store) {
                generator = async function*() {
                    for await (const docId of docIds) {
                        try {
                            const sourceDoc = store.get(docId - fromIndex) as T

                            const doc: ResultItem<T> = {
                                _id: docId,
                                _source: sourceDoc
                            }

                            yield doc
                        } catch (e) {
                            console.log('fail to load:', docId)
                            throw e
                        }
                    }
                }
            } else {
                generator = async function*() {
                    for await (const docId of docIds) {
                        yield {
                            _id: docId
                        }
                    }
                }
            }

            return from(generator())
        } else {
            let store: DocPackedArray
            let generator
            if (this.hasStoredFields) {
                store = this.perFieldValue
                generator = async function*() {
                    for await (const docId of docIds) {
                        const doc: ResultItem<T> = {
                            _id: docId
                        }

                        const allStoredFields = store.get(docId) as Doc

                        for (let i = 0; i < projection.length; i++) {
                            const fieldName = projection[i]
                            doc[fieldName] = allStoredFields[fieldName] as FieldValue | FieldValues | FieldStorableValue
                        }

                        yield doc
                    }
                }
            } else {
                generator = async function*() {
                    for await (const docId of docIds) {
                        const doc: ResultItem<T> = {
                            _id: docId
                        }

                        yield doc
                    }
                }
            }

            return from(generator())
        }
    }

    get(field: FieldName, term: Buffer): DocIdAsyncIterable {
        const fieldMaps = this.perFieldMap.get(field)
        const map = fieldMaps?.get(term.toString('base64'))

        if (typeof map === 'number') {
            return new SingletonDocIdAsyncIterable(map)
        } else if (map) {
            return new BitmapAsyncIterable(map, false)
        } else {
            return BitmapAsyncIterable.EMPTY_MAP
        }
    }

    getOptimizedComparator(fieldName: FieldName): ICompareFunction<DocId> | undefined {
        return this.perSortedFieldValue.get(fieldName)?.comparator
    }
}
