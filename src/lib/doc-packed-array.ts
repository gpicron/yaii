import { Doc } from '../yaii-types'
import { RoaringBitmap32 } from 'roaring'
import * as util from 'util'
import ByteBuffer = require('bytebuffer')

enum FieldTag {
    Boolean,
    Numeric,
    NumericArray,
    String,
    StringArray,
    Child,
    ChildArray,
    BufferValue,
    MixedArray
}

interface FieldConfig {
    id: FieldNumber
    kind: FieldTag
    docType?: DocType
}

type FieldNumber = number
type FieldName = string

type DocType = {
    fields: Map<FieldName, FieldConfig[]>
    lastId: number
    mapConfigs?: FieldConfig[]
}

function generateCodeForEncofingFieldConfig(
    fieldConfig: FieldConfig,
    fieldName: FieldName,
    root: string
) {
    let code = ''
    switch (fieldConfig.kind) {
        case FieldTag.Boolean:
            code += `if (data === true) {store.writeUint8(0x01)} else if (data === false) {store.writeUint8(0x02)} else throw new Error("not a boolean");`
            break
        case FieldTag.Numeric:
            // TODO improve encoding (similar as varint but for general number)
            code += `store.writeDouble(data);`
            break
        case FieldTag.NumericArray:
            code += `store.writeVarint32(data.length);`
            code += 'for (const val of data) {'
            // TODO improve encoding (similar as varint but for general number)
            code += `  store.writeDouble(val);`
            code += '}'
            break
        case FieldTag.String:
            code += `store.writeVString(data);`
            break
        case FieldTag.StringArray:
            code += `store.writeVarint32(data.length);`
            code += 'for (const val of data) {'
            code += `  store.writeVString(val);`
            code += '}'
            break
        case FieldTag.Child:
            code += '{'
            code += '  const doc = data;'
            code += '  if (doc === null) {'
            code += '    store.writeVarint32(-1);'
            code += '  } else {'
            code += generateEncoderCodeForType(
                fieldConfig.docType as DocType,
                `${root}.${fieldName}`
            )
            code += '  }'
            code += '}'
            break
        case FieldTag.ChildArray:
            code += 'store.writeVarint32(data.length);'
            code += 'for (const doc of data) {'
            code += '  if (doc === null) {'
            code += '    store.writeVarint32(-1);'
            code += '  } else {'
            code += generateEncoderCodeForType(
                fieldConfig.docType as DocType,
                `${root}.${fieldName}`
            )
            code += '  }'
            code += '}'
            break
        case FieldTag.MixedArray:
            code += 'function encodeMixedArray(array) {'
            code += '  store.writeVarint32(array.length);'
            code += '  for (const el of array) {'
            code += '    if (el === null) {'
            code += '      store.writeVarint32(-1);'
            code += '    } else {'
            code += '      const typeofdoc = typeof el;'
            code += "      if (typeofdoc === 'number') {"
            code += `        store.writeVarint32(${FieldTag.Numeric});`
            code += '        store.writeDouble(el);'
            code += "      } else if (typeofdoc === 'string') {"
            code += `        store.writeVarint32(${FieldTag.String});`
            code += '        store.writeVString(el);'
            code += '      } else if (Array.isArray(el)) {'
            code += `        store.writeVarint32(${FieldTag.MixedArray});`
            code += '        encodeMixedArray(el);'
            code += '      } else {'
            code += `        store.writeVarint32(${FieldTag.Child});`
            code += '        {'
            code += '          const doc = el;'
            code += '          if (doc === null) {'
            code += '            store.writeVarint32(-1);'
            code += '          } else {'
            code += generateEncoderCodeForType(
                fieldConfig.docType as DocType,
                `${root}.${fieldName}`
            )
            code += '          }'
            code += '        }'
            code += '      }'
            code += '    }'
            code += '  } /* end for */'
            code += '} /* end function */'
            code += 'encodeMixedArray(data);'

            break

        default:
            throw new Error('not yet implemented for ' + fieldConfig.kind)
    }
    return code
}

function generateEncoderForPrimitiveArrays(
    primitiveArrayFieldConfigs: FieldConfig[],
    writeFieldName: boolean,
    fieldName: FieldName,
    root: string
) {
    let code = ''
    if (primitiveArrayFieldConfigs.length == 1) {
        code += `store.writeVarint32(${primitiveArrayFieldConfigs[0].id});`
        if (writeFieldName) code += `store.writeVString(field);`
        code += generateCodeForEncofingFieldConfig(
            primitiveArrayFieldConfigs[0],
            fieldName,
            root
        )
    } else {
        code += 'if (data.length == 0) {'
        code += `store.writeVarint32(${primitiveArrayFieldConfigs[0].id});`
        if (writeFieldName) code += `store.writeVString(field);`
        code += generateCodeForEncofingFieldConfig(
            primitiveArrayFieldConfigs[0],
            fieldName,
            root
        )
        for (const pfc of primitiveArrayFieldConfigs) {
            switch (pfc.kind) {
                case FieldTag.NumericArray:
                    code += '} else if (typeof data[0] === "numeric") {'
                    code += `store.writeVarint32(${pfc.id});`
                    if (writeFieldName) code += `store.writeVString(field);`
                    code += generateCodeForEncofingFieldConfig(
                        pfc,
                        fieldName,
                        root
                    )
                    break
                case FieldTag.StringArray:
                    code += '} else if (typeof data[0] === "string") {'
                    code += `store.writeVarint32(${pfc.id});`
                    if (writeFieldName) code += `store.writeVString(field);`
                    code += generateCodeForEncofingFieldConfig(
                        pfc,
                        fieldName,
                        root
                    )
                    break
            }
        }
        code += '}'
    }
    return code
}

function generateEncoderCodeForRecordArray(
    arrayChildFieldConfig: FieldConfig,
    writeFieldName: boolean,
    fieldName: FieldName,
    root: string
) {
    let code = `store.writeVarint32(${arrayChildFieldConfig.id});`
    if (writeFieldName) code += `store.writeVString(field);`
    code += generateCodeForEncofingFieldConfig(
        arrayChildFieldConfig,
        fieldName,
        root
    )
    return code
}

function generateEncoderCodeForChildRecord(
    childFieldConfig: FieldConfig,
    writeFieldName: boolean,
    fieldName: FieldName,
    root: string
) {
    let code = `store.writeVarint32(${childFieldConfig.id});`
    if (writeFieldName) code += `store.writeVString(field);`
    code += generateCodeForEncofingFieldConfig(
        childFieldConfig,
        fieldName,
        root
    )
    return code
}

function generateCodeForEncondingMultiTypeField(
    fieldConfigs: FieldConfig[],
    fieldName: FieldName,
    root: string,
    writeFieldName: boolean = false
) {
    let code = 'switch (typeof data) {'

    const primitiveFieldConfigs = fieldConfigs.filter(
        f =>
            f.kind == FieldTag.Boolean ||
            f.kind == FieldTag.String ||
            f.kind == FieldTag.Numeric
    )

    for (const fieldConfig of primitiveFieldConfigs) {
        switch (fieldConfig.kind) {
            case FieldTag.Boolean:
                code += "case 'boolean': {"
                break
            case FieldTag.String:
                code += "case 'string': {"
                break
            case FieldTag.Numeric:
                code += "case 'number': {"
                break
        }
        code += `store.writeVarint32(${fieldConfig.id});`
        if (writeFieldName) code += `store.writeVString(field);`
        code += generateCodeForEncofingFieldConfig(fieldConfig, fieldName, root)
        code += 'break; }'
    }

    const childFieldConfig = fieldConfigs.find(f => f.kind == FieldTag.Child)
    const arrayChildFieldConfig = fieldConfigs.find(
        f => f.kind == FieldTag.ChildArray
    )
    const primitiveArrayFieldConfigs = fieldConfigs.filter(
        f => f.kind == FieldTag.StringArray || f.kind == FieldTag.NumericArray
    )

    const mixedArrayFieldConfig: FieldConfig | undefined = fieldConfigs.find(
        f => f.kind == FieldTag.MixedArray
    )
    const bufferFieldConfigs = fieldConfigs.filter(
        f => f.kind == FieldTag.BufferValue
    )
    if (bufferFieldConfigs.length > 0) throw new Error('Not yet implemented')

    const hasPrimitiveArrayConfig = primitiveArrayFieldConfigs.length > 0

    if (
        childFieldConfig ||
        hasPrimitiveArrayConfig ||
        arrayChildFieldConfig ||
        mixedArrayFieldConfig
    ) {
        code += "case 'object': {"

        code += 'if (Array.isArray(data)) {'

        if (mixedArrayFieldConfig) {
            code += `store.writeVarint32(${mixedArrayFieldConfig.id});`
            if (writeFieldName) code += `store.writeVString(field);`
            code += generateCodeForEncofingFieldConfig(
                mixedArrayFieldConfig,
                fieldName,
                root
            )
        } else {
            code += '  if (data.length === 0) {'
            if (hasPrimitiveArrayConfig) {
                code += generateEncoderForPrimitiveArrays(
                    primitiveArrayFieldConfigs,
                    writeFieldName,
                    fieldName,
                    root
                )
            } else if (arrayChildFieldConfig) {
                code += generateEncoderCodeForRecordArray(
                    arrayChildFieldConfig,
                    writeFieldName,
                    fieldName,
                    root
                )
            } else {
                code += "throw new Error('array types not yet supported')"
            }
            code +=
                "  } else if (typeof data[0] === 'number' || typeof data[0] === 'string') {"
            if (hasPrimitiveArrayConfig) {
                code += generateEncoderForPrimitiveArrays(
                    primitiveArrayFieldConfigs,
                    writeFieldName,
                    fieldName,
                    root
                )
            } else {
                code +=
                    "throw new Error('primitive array type not yet supported')"
            }
            code += "  } else if (typeof data[0] === 'object') {"
            if (arrayChildFieldConfig) {
                code += generateEncoderCodeForRecordArray(
                    arrayChildFieldConfig,
                    writeFieldName,
                    fieldName,
                    root
                )
            } else {
                code += "throw new Error('record array type not yet supported')"
            }
            code += '  } else {'
            code += "    throw new Error('bug')"
            code += '  }'
        }
        code += "} else if (typeof data === 'object') {"
        if (childFieldConfig) {
            code += generateEncoderCodeForChildRecord(
                childFieldConfig,
                writeFieldName,
                fieldName,
                root
            )
        } else {
            code += "throw new Error('child record type not yet supported')"
        }
        code += '} else {'
        code += "throw new Error('bug')"
        code += '}'

        code += 'break; }'
    }

    code += 'default: throw new Error()'

    code += '}'
    return code
}

function generateEncoderCodeForType(type: DocType, root = '') {
    let code = `for (const [field, data] of Object.entries(doc)) {`

    code += ' switch (field) {'
    for (const [fieldName, fieldConfigs] of type.fields.entries()) {
        code += `case "${fieldName}": {`

        if (fieldConfigs.length == 1) {
            const fieldConfig = fieldConfigs[0]
            code += `store.writeVarint32(${fieldConfig.id});`
            code += generateCodeForEncofingFieldConfig(
                fieldConfig,
                fieldName,
                root
            )
        } else {
            code += generateCodeForEncondingMultiTypeField(
                fieldConfigs,
                fieldName,
                root
            )
        }

        code += ' break; } '
    }
    if (type.mapConfigs) {
        code += ' default:'
        const fieldConfigs = type.mapConfigs

        if (fieldConfigs.length == 1) {
            const fieldConfig = fieldConfigs[0]
            code += `store.writeVarint32(${fieldConfig.id});`
            code += `store.writeVString(field);`
            code += generateCodeForEncofingFieldConfig(
                fieldConfig,
                '£_mapped',
                root
            )
        } else {
            code += generateCodeForEncondingMultiTypeField(
                fieldConfigs,
                '£_mapped',
                root,
                true
            )
        }
    } else {
        code += `  default: throw new Error('failure with field ${root}.' + field)`
    }
    code += '}'

    code += '}'
    code += 'store.writeVarint32(0x00);\n'

    return code
}

function generateCodeForDecodingField(
    fieldConfig: FieldConfig,
    fieldName: string | null
) {
    let code = `case ${fieldConfig.id}: {`

    let fieldIndex
    if (fieldName === null) {
        code +=
            'const nextFieldIndex = store.readVString(pointer); pointer+=nextFieldIndex.length;'
        code += `const fieldIndex = nextFieldIndex.string;`
        fieldIndex = `fieldIndex`
    } else {
        fieldIndex = `'${fieldName}'`
    }

    switch (fieldConfig.kind) {
        case FieldTag.Boolean:
            code += 'const nextRead = store.readUint8(pointer); pointer+=1;'
            code += `result[${fieldIndex}] = (nextRead === 0x01) ? true : false;`
            break
        case FieldTag.Numeric:
            code += 'const nextRead = store.readDouble(pointer); pointer+=8;'
            code += `result[${fieldIndex}] = nextRead;`
            break
        case FieldTag.NumericArray:
            code +=
                'const lenRead = store.readVarint32(pointer); pointer+=lenRead.length;'
            code += 'const len = lenRead.value;'
            code += 'const array = [];'
            code += 'for (let i = 0; i < len; i++) {'
            code += 'const nextRead = store.readDouble(pointer); pointer+=8;'
            code += 'array[i]=nextRead;'
            code += '}'
            code += `result[${fieldIndex}] = array;`
            break
        case FieldTag.String:
            code +=
                'const nextRead = store.readVString(pointer); pointer+=nextRead.length;'
            code += `result[${fieldIndex}] = nextRead.string;`
            break
        case FieldTag.StringArray:
            code +=
                'const lenRead = store.readVarint32(pointer); pointer+=lenRead.length;'
            code += 'const len = lenRead.value;'
            code += 'const array = [];'
            code += 'for (let i = 0; i < len; i++) {'
            code +=
                'const nextRead = store.readVString(pointer); pointer+=nextRead.length;'
            code += 'array[i]=nextRead.string;'
            code += '}'
            code += `result[${fieldIndex}] = array;`
            break
        case FieldTag.Child:
            code += 'const parent = result; {'
            code += generateDecoderCodeForType(fieldConfig.docType as DocType)
            code += `parent[${fieldIndex}] = result; }`
            break
        case FieldTag.ChildArray:
            code += 'const parent = result; {'
            code +=
                'const lenRead = store.readVarint32(pointer); pointer+=lenRead.length;'
            code += 'const len = lenRead.value;'
            code += 'const array = [];'
            code += 'for (let i = 0; i < len; i++) {'
            code += generateDecoderCodeForType(fieldConfig.docType as DocType)
            code += 'array[i]=result;'
            code += '}'
            code += `parent[${fieldIndex}] = array; }`
            break
        case FieldTag.MixedArray:
            code += 'function decodeMixedArray() {'

            code +=
                'const lenRead = store.readVarint32(pointer); pointer+=lenRead.length;'
            code += 'const len = lenRead.value;'
            code += 'const array = [];'

            code += 'for (let i = 0; i < len; i++) {'

            code +=
                '  const elTypeRead = store.readVarint32(pointer); pointer+=elTypeRead.length;'
            code += '  const elType = elTypeRead.value;'
            code += '  switch (elType) {'
            code += `    case (${FieldTag.Numeric}): {`
            code +=
                '      const nextRead = store.readDouble(pointer); pointer+=8;'
            code += '      array[i] = nextRead;'
            code += '      break;'
            code += '    }'
            code += `    case (${FieldTag.String}): {`
            code +=
                '      const nextRead = store.readVString(pointer); pointer+=nextRead.length;'
            code += '      array[i] = nextRead.string;'
            code += '      break;'
            code += '    }'
            code += `    case (${FieldTag.Child}): {`
            code += '      const parent = array; {'
            code += generateDecoderCodeForType(fieldConfig.docType as DocType)

            code += `      parent[i] = result; `
            code += '      }'
            code += '      break;'
            code += '    }'
            code += `    case (${FieldTag.MixedArray}): {`
            code += `      array[i] = decodeMixedArray();`
            code += '      break;'
            code += '    }'
            code += "    default: throw new Error('bug')"
            code += '  } /* end switch on elType */'
            code += '} /* end for*/'

            code += 'return array;'
            code += '} /* function */'
            code += `result[${fieldIndex}] = decodeMixedArray(); `
            break
    }
    code += 'break; } /* end case */'
    return code
}

function generateDecoderCodeForType(type: DocType) {
    let code = ''
    code += `let result = {};`
    code += `const nextRead = store.readVarint32(pointer); pointer+=nextRead.length; let fieldId=nextRead.value; `
    code += 'if (fieldId === -1) {'
    code += '   result == null'
    code += '} else {'

    code += `while (fieldId !== 0x00) {`

    code += ' switch (fieldId) {'
    for (const [fieldName, fieldConfigs] of type.fields.entries()) {
        for (const fieldConfig of fieldConfigs) {
            code += generateCodeForDecodingField(fieldConfig, fieldName)
        }
        if (type.mapConfigs) {
            for (const fieldConfig of type.mapConfigs) {
                code += generateCodeForDecodingField(fieldConfig, null)
            }
        }
    }
    code += '} /* end switch */'
    code += `const nextRead = store.readVarint32(pointer); pointer+=nextRead.length; fieldId=nextRead.value; `
    code += '} /* end while */'
    code += '} /* end else */'

    return code
}

type GeneratedEncoder = {
    function: (doc: Doc, store: ByteBuffer) => void
    code: string
}

function generateEncoder(type: DocType): GeneratedEncoder {
    const code = generateEncoderCodeForType(type)

    try {
        return {
            function: new Function('doc', 'store', code) as (
                doc: Doc,
                store: ByteBuffer
            ) => void,
            code: code
        }
    } catch (e) {
        console.error('Issue in generated code for encoder:', code)
        throw e
    }
}

type GeneratedDecoder = {
    function: (pointer: number, store: ByteBuffer) => Doc
    code: string
}

function generateDecoder(type: DocType): GeneratedDecoder {
    let code = generateDecoderCodeForType(type)
    code += 'return result;'

    try {
        return {
            function: new Function('pointer', 'store', code) as (
                pointer: number,
                store: ByteBuffer
            ) => Doc,
            code: code
        }
    } catch (e) {
        console.error('Issue in generated code for decoder:', code)
        throw e
    }
}

function upgradeSchema(
    doc: object,
    current: DocType,
    targetMaxFieldTagsPerLevel: number
) {
    if (!doc) {
        throw new Error()
    }
    for (const [field, data] of Object.entries(doc)) {
        let fieldConfigs = current.fields.get(field)

        if (!fieldConfigs && current.mapConfigs) {
            fieldConfigs = current.mapConfigs
        }

        let fieldConfig
        let neededKind
        let typesInArray

        switch (typeof data) {
            case 'boolean':
                neededKind = FieldTag.Boolean
                fieldConfig = fieldConfigs?.find(
                    fc => fc.kind == FieldTag.Boolean
                )
                break
            case 'number':
                neededKind = FieldTag.Numeric
                fieldConfig = fieldConfigs?.find(
                    fc => fc.kind == FieldTag.Numeric
                )
                break
            case 'string':
                neededKind = FieldTag.String
                fieldConfig = fieldConfigs?.find(
                    fc => fc.kind == FieldTag.String
                )
                break
            case 'object':
                if (Array.isArray(data)) {
                    typesInArray = data.reduce((acc, el) => {
                        acc.set(typeof el, true)
                        return acc
                    }, new Map<string, boolean>())
                    if (typesInArray.size === 0) {
                        fieldConfig = fieldConfigs?.find(
                            fc =>
                                fc.kind == FieldTag.NumericArray ||
                                fc.kind == FieldTag.StringArray ||
                                fc.kind == FieldTag.ChildArray ||
                                fc.kind == FieldTag.MixedArray
                        )
                        if (fieldConfig) {
                            neededKind = fieldConfig.kind
                        } else {
                            neededKind = FieldTag.StringArray
                        }
                    } else if (typesInArray.size === 1) {
                        switch (typesInArray.keys().next().value) {
                            case 'number':
                                neededKind = FieldTag.NumericArray
                                fieldConfig = fieldConfigs?.find(
                                    fc => fc.kind == FieldTag.NumericArray
                                )
                                break
                            case 'string':
                                neededKind = FieldTag.StringArray
                                fieldConfig = fieldConfigs?.find(
                                    fc => fc.kind == FieldTag.StringArray
                                )
                                break
                            case 'object':
                                neededKind = FieldTag.ChildArray
                                fieldConfig = fieldConfigs?.find(
                                    fc => fc.kind == FieldTag.ChildArray
                                )
                                break
                            default:
                                throw new Error('not yet implemented')
                        }
                    } else {
                        neededKind = FieldTag.MixedArray
                        fieldConfig = fieldConfigs?.find(
                            fc => fc.kind == FieldTag.MixedArray
                        )
                    }
                } else if (Buffer.isBuffer(data)) {
                    neededKind = FieldTag.BufferValue
                    fieldConfig = fieldConfigs?.find(
                        fc => fc.kind == FieldTag.BufferValue
                    )
                } else {
                    neededKind = FieldTag.Child
                    fieldConfig = fieldConfigs?.find(
                        fc => fc.kind == FieldTag.Child
                    )
                }
        }

        if (!fieldConfig && neededKind !== undefined) {
            if (!fieldConfigs) {
                if (current.lastId >= targetMaxFieldTagsPerLevel) {
                    current.mapConfigs = fieldConfigs = new Array<FieldConfig>()
                } else {
                    fieldConfigs = new Array<FieldConfig>()
                    current.fields.set(field, fieldConfigs)
                }
            }

            current.lastId++
            let childDocType

            if (
                neededKind == FieldTag.Child ||
                neededKind == FieldTag.ChildArray
            ) {
                childDocType = {
                    fields: new Map<FieldName, FieldConfig[]>(),
                    lastId: 0
                }

                if (Array.isArray(data)) {
                    for (const el of data) {
                        if (el !== null)
                            upgradeSchema(
                                el,
                                childDocType,
                                targetMaxFieldTagsPerLevel
                            )
                    }
                } else if (data !== null) {
                    upgradeSchema(
                        data,
                        childDocType,
                        targetMaxFieldTagsPerLevel
                    )
                }
            } else if (neededKind == FieldTag.MixedArray) {
                childDocType = {
                    fields: new Map<FieldName, FieldConfig[]>(),
                    lastId: 0
                }

                function traverseArray(array: [], type: DocType) {
                    for (const el of array) {
                        if (el !== null && typeof el === 'object') {
                            if (Array.isArray(el)) {
                                traverseArray(el, type)
                            } else {
                                upgradeSchema(
                                    el,
                                    type,
                                    targetMaxFieldTagsPerLevel
                                )
                            }
                        }
                    }
                }

                traverseArray(data, childDocType)
            }

            fieldConfigs.push({
                id: current.lastId,
                kind: neededKind,
                docType: childDocType
            })
        } else if (fieldConfig?.docType) {
            if (Array.isArray(data)) {
                for (const el of data) {
                    if (el !== null)
                        upgradeSchema(
                            el,
                            fieldConfig.docType,
                            targetMaxFieldTagsPerLevel
                        )
                }
            } else if (data !== null) {
                upgradeSchema(
                    data,
                    fieldConfig.docType,
                    targetMaxFieldTagsPerLevel
                )
            }
        }
    }
}

export class DocPackedArray {
    private pointers = new RoaringBitmap32()
    store: ByteBuffer

    private rootSchema: DocType = {
        fields: new Map(),
        lastId: 0
    }
    private encoder: GeneratedEncoder | undefined
    private decoder: GeneratedDecoder | undefined
    private targetMaxFieldTagsPerLevel: number

    constructor(targetMaxFieldTagsPerLevel: number = 127) {
        this.targetMaxFieldTagsPerLevel = targetMaxFieldTagsPerLevel
        this.store = ByteBuffer.allocate(4 * 1024, true)
    }

    add(doc: Doc): void {
        try {
            this.store.mark()

            if (!this.encoder) throw Error('not yet generated')

            this.encoder.function(doc, this.store)

            // BB do not extend the limit when expending the capacity
            this.store.limit = this.store.capacity()
        } catch (e) {
            this.store.reset()
            upgradeSchema(doc, this.rootSchema, this.targetMaxFieldTagsPerLevel)
            this.encoder = generateEncoder(this.rootSchema)
            this.decoder = generateDecoder(this.rootSchema)

            try {
                this.store.mark()
                this.encoder.function(doc, this.store)

                // BB do not extend the limit when expending the capacity
                this.store.limit = this.store.capacity()
            } catch (e) {
                this.store.reset()
                throw new Error(
                    `Maybe issue in generated code:\n${e}\n\ncode:\n${
                        this.encoder.code
                    }\n\nobject:\n${util.inspect(doc, false, null, true)}`
                )
            }
        }

        this.pointers.add(this.store.offset)
    }

    get(index: number): Doc | undefined {
        const pointer = this.pointers.select(index - 1) || 0

        return this.decoder?.function(pointer, this.store)
    }

    get length(): number {
        return this.pointers.size
    }
}
