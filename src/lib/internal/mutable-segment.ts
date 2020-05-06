import {DocId} from '../../yaii-types'
import { AsyncIterableX, count, from } from 'ix/asynciterable'
import { RoaringBitmap32 } from 'roaring'
import * as op from 'ix/asynciterable/operators'
import { ExtFieldConfig, ExtFieldsIndexConfig, ExtIndexConfig, ICompareFunction } from './utils'
import { SingletonDocidAsyncIterable } from './datastructs/docid-async-iterable/singleton-docid-async-iterable'
import { IndexableDoc } from '../..'
import { DocPackedArray } from './datastructs/doc-packed-array'
import { INTERNAL_FIELDS } from './utils'
import { SortFieldPackedArray } from './datastructs/sort-field-packed-array'
import {Doc, FieldName, FieldStorableValue, FieldValue, FieldValues, ResultItem} from "../api/base"
import {FieldConfigFlag} from "../api/config"
import {DocidAsyncIterable} from "./datastructs/docid-async-iterable/docid-async-iterable"
import {BitmapDocidAsyncIterable} from "./datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {Term, TermExp} from "./query-ir/term-exp"
import {stringToTerm} from "./query-ir/query-ir"

export class MutableSegment {
    private fieldsIndexConfig: ExtFieldsIndexConfig
    private indexConfig: ExtIndexConfig
    private perFieldMap: Map<FieldName, Map<Term, RoaringBitmap32 | number>> = new Map()
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

    private setupStorageForField(field: FieldName, fConfig: ExtFieldConfig): void {
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

        const docSourceStore = this.docSourceStore
        const fieldsIndexConfig = this.fieldsIndexConfig
        const indexConfig = this.indexConfig
        const hasStoredFields = this.hasStoredFields
        const perFieldValue = this.perFieldValue
        const perFieldMap = this.perFieldMap
        const perSortedFieldValue = this.perSortedFieldValue

        const insertDocument = async (doc: IndexableDoc, index: number) => {
            index += start

            const source = doc[INTERNAL_FIELDS.SOURCE]

            if (source && docSourceStore) {
                docSourceStore.add(source)
            }

            const nonEmptyFields = new Map<string, boolean>()
            const storedFields: Doc = {}


            for (const [fieldName, fieldValue] of Object.entries(doc)) {
                if (typeof fieldValue !== 'undefined') {
                    let conf = fieldsIndexConfig[fieldName]

                    if (!conf && indexConfig.defaultFieldConfig) {
                        conf = indexConfig.defaultFieldConfig

                        this.setupStorageForField(fieldName, conf)

                        fieldsIndexConfig[fieldName] = conf
                    }

                    if (conf) {
                        // update value columns
                        if (conf.flags & FieldConfigFlag.STORED) {
                            storedFields[fieldName] = fieldValue

                            nonEmptyFields.set(fieldName, true)
                        }

                        // update bitmaps
                        if (conf.flags & FieldConfigFlag.SEARCHABLE && !Buffer.isBuffer(fieldValue)) {
                            const terms = conf.tokenizer(fieldValue)

                            if (terms.length > 0) {
                                const mapTree = perFieldMap.get(fieldName) as Map<Term, RoaringBitmap32 | number>
                                for (const term of terms) {
                                    const key = term
                                    const map = mapTree.get(key)
                                    if (map == undefined) {
                                        mapTree.set(key, index)
                                    } else if (typeof map === 'number') {
                                        mapTree.set(key, RoaringBitmap32.from([map, index]))
                                    } else {
                                        map.add(index)
                                    }
                                }

                                nonEmptyFields.set(fieldName, true)
                            }
                        }

                        // update sort fields
                        if (conf.flags & FieldConfigFlag.SORT_OPTIMIZED && !Buffer.isBuffer(fieldValue)) {
                            const val = Array.isArray(fieldValue) ? fieldValue[0] : fieldValue

                            const array = perSortedFieldValue.get(fieldName) as SortFieldPackedArray
                            array.add(val)
                        }
                    }
                }
            }
            // index non empty fields list
            if (nonEmptyFields.size > 0) {
                const mapTree = perFieldMap.get(INTERNAL_FIELDS.FIELDS) as Map<Term, RoaringBitmap32>

                for (const key of nonEmptyFields.keys()) {
                    const term = stringToTerm(key)
                    const map = mapTree.get(term)
                    if (map == undefined) {
                        mapTree.set(term, RoaringBitmap32.from([index]))
                    } else {
                        map.add(index)
                    }
                }
            }

            if (hasStoredFields && storedFields) {
                perFieldValue.add(storedFields)
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

    get(field: FieldName, term: Term): DocidAsyncIterable {
        const fieldMaps = this.perFieldMap.get(field)
        const map = fieldMaps?.get(term)

        if (typeof map === 'number') {
            return new SingletonDocidAsyncIterable(map)
        } else if (map) {
            return new BitmapDocidAsyncIterable(false, map)
        } else {
            return BitmapDocidAsyncIterable.EMPTY_MAP
        }
    }

    getOptimizedComparator(fieldName: FieldName): ICompareFunction<DocId> | undefined {
        return this.perSortedFieldValue.get(fieldName)?.comparator
    }

    mayMatch(term: TermExp): boolean {
        return this.perFieldMap.get(term.field)?.has(term.term) !== undefined
    }

}
