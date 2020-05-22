import {DocId, SortClause} from '../../../yaii-types'
import {as, AsyncIterableX, empty, from} from 'ix/asynciterable'
import {RoaringBitmap32} from 'roaring'
import * as op from 'ix/asynciterable/operators'
import {ExtFieldConfig, ExtFieldsIndexConfig, ExtIndexConfig, INTERNAL_FIELDS} from '../utils'
import {SingletonDocidAsyncIterable} from '../datastructs/docid-async-iterable/singleton-docid-async-iterable'
import {IndexableDoc} from '../../..'
import {DocPackedArray} from '../datastructs/doc-packed-array'
import {Doc, FieldName, FieldStorableValue, FieldValue, FieldValues, ResultItem} from "../../api/base"
import {FieldConfigFlag} from "../../api/config"
import {BitmapDocidAsyncIterable} from "../datastructs/docid-async-iterable/bitmap-docid-async-iterable"
import {Term, TermExp} from "../query-ir/term-exp"
import {stringToTerm} from "../query-ir/query-ir"
import {BaseSegment} from "./segment"
import {buildComparatorAndProjections, ProjectionsAndComparator} from "../../../base-inverted-index"
import {DocIdIterable} from "../datastructs/docid-async-iterable/base"
import {EMPTY_MAP} from "../datastructs/docid-async-iterable/range-docid-async-iterable"

export class MutableSegment extends BaseSegment {
    readonly perFieldMap: Map<FieldName, Map<Term, RoaringBitmap32 | number>> = new Map()
    readonly perFieldValue = DocPackedArray.createNew(127)
    readonly docSourceStore: DocPackedArray | undefined
    private _next: number

    private hasStoredFields: boolean = false

    private _deleted = new BitmapDocidAsyncIterable()

    constructor(id: number, fieldsIndexConfig: ExtFieldsIndexConfig, indexConfig: ExtIndexConfig, from: number) {
        super(id, fieldsIndexConfig, indexConfig, from)

        this._next = from

        if (this.indexConfig.storeSourceDoc) {
            this.docSourceStore = DocPackedArray.createNew()
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

        if (fConfig.flags & FieldConfigFlag.STORED || fConfig.flags & FieldConfigFlag.SORT_OPTIMIZED) {
            this.hasStoredFields = true
        }
    }

    add(docs: IndexableDoc[]): number {
        const start = this.next

        const docSourceStore = this.docSourceStore
        const fieldsIndexConfig = this.fieldsIndexConfig
        const indexConfig = this.indexConfig
        const hasStoredFields = this.hasStoredFields
        const perFieldMap = this.perFieldMap

        if (docSourceStore) {
            docSourceStore.addAll(docs.map(it => it[INTERNAL_FIELDS.SOURCE]) as Doc[])
        }

        let bufferStoredField: Doc[] | undefined
        if (hasStoredFields) {
            bufferStoredField = new Array<Doc>(docs.length)
        }


        for (let i=0; i < docs.length; i++) {
            const doc = docs[i]

            const index = start + i

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
                        if (conf.flags & FieldConfigFlag.STORED || conf.flags & FieldConfigFlag.SORT_OPTIMIZED) {
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

            if (bufferStoredField) {
                bufferStoredField[i] = storedFields
            }
        }

        if (bufferStoredField) {
            this.perFieldValue.addAll(bufferStoredField)
        }

        this._next += docs.length

        return docs.length
    }

    get rangeSize() {
        return this.next - this.from
    }

    get next() {
        return this._next
    }

    get(field: FieldName, term: Term): DocIdIterable {
        const fieldMaps = this.perFieldMap.get(field)
        const map = fieldMaps?.get(term)

        if (typeof map === 'number') {
            return new SingletonDocidAsyncIterable(map)
        } else if (map) {
            return new BitmapDocidAsyncIterable(false, map)
        } else {
            return EMPTY_MAP
        }
    }

    mayMatch(term: TermExp): boolean {
        return this.perFieldMap.get(term.field)?.has(term.term) !== undefined
    }

    terms(field: FieldName): AsyncIterableX<Term> {
        const mapforField = this.perFieldMap.get(field)
        if (mapforField) {
            return from(mapforField.keys()).pipe(
                op.orderBy(item => item)
            );
        } else {
            return empty()
        }
    }

    deleted(): BitmapDocidAsyncIterable {
        return this._deleted.readOnly()
    }

    addToDeleted(docId: DocId): void {
        this._deleted.add(docId)
    }

    addProjections<T extends Doc>(source: AsyncIterable<ResultItem<T>>, projection?: string[]): AsyncIterableX<ResultItem<T>> {
        if (projection == undefined) {
            const store = this.docSourceStore
            let generator
            if (store) {
                generator = async function* () {
                    for await (const doc of source) {
                        try {
                            const sourceDoc = store.get(doc._id) as T

                            doc._source = sourceDoc

                            yield doc
                        } catch (e) {
                            console.log('fail to load:', doc._id)
                            throw e
                        }
                    }
                }
                return from(generator())
            } else {
                return as(source)
            }

        } else {
            let store: DocPackedArray
            let generator
            if (this.hasStoredFields) {
                store = this.perFieldValue
                generator = async function* () {
                    for await (const doc of source) {

                        const allStoredFields = store.get(doc._id) as Doc

                        for (let i = 0; i < projection.length; i++) {
                            const fieldName = projection[i]
                            doc[fieldName] = allStoredFields[fieldName] as FieldValue | FieldValues | FieldStorableValue
                        }

                        yield doc
                    }
                }
                return from(generator())
            } else {
                return as(source)
            }


        }
    }

    buildComparatorAndProjections(sort: Array<SortClause>): ProjectionsAndComparator {
        return buildComparatorAndProjections(sort);
    }


}
