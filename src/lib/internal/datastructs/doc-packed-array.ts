import {Doc} from '../../api/base'
import {RoaringBitmap32} from 'roaring'
import * as util from 'util'
import {ByteBufferAccess, ValueStoreType} from "./stores/base"
import {ValueWithMemoryEstimation} from "./lru-cache"
import ByteBuffer = require('bytebuffer')

export enum FieldTag {
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

enum FieldFlags {
    NUMERIC_SIGNED = 1,
    NUMERIC_64BIT = 1 << 2,
    NUMERIC_FLOAT = 1 << 3,
    STRING_FIX_LEN = 1,
}

export class FieldValueConfig {
    id: FieldNumber
    kind: FieldTag
    elementSchema?: RecordSchema
    flags: number
    len?: number

    constructor(id: FieldNumber, kind: FieldTag, elementSchema?: RecordSchema) {
        this.id = id
        this.kind = kind
        this.elementSchema = elementSchema
        this.flags = 0
    }

    updateFlags(input: unknown) {
        switch (this.kind) {
            case FieldTag.Numeric:
                this.setNumberFlags(input as number)
                break;
            case FieldTag.NumericArray:
                for (const e of (input as Array<number>)) {
                    this.setNumberFlags(e)
                }
                break;
            case FieldTag.String:
                this.setStringFlags(input as string)
                break;
        }
    }

    setNumberFlags(input: number): void {
        let fl = this.flags

        if ((fl & FieldFlags.NUMERIC_FLOAT) != 0) return
        if ((fl & FieldFlags.NUMERIC_SIGNED) != 0 && (fl & FieldFlags.NUMERIC_64BIT) != 0) return

        if (Number.isInteger(input)) {
            if (input < 0) {
                fl |= FieldFlags.NUMERIC_SIGNED
                if (input < -2147483648  || input > 2147483647) {
                    fl |= FieldFlags.NUMERIC_64BIT
            }
            } else {
                if (input > 4294967295) {
                    fl |= FieldFlags.NUMERIC_64BIT
            }
            }
        } else {
            fl |= FieldFlags.NUMERIC_FLOAT
            }

        this.flags = fl
        }

    setStringFlags(input: string): void {
        if (this.len) {
            if (input.length !== this.len) {
                this.flags = 0
                this.len = undefined
            }
        } else {
            this.flags |= FieldFlags.STRING_FIX_LEN
            this.len = input.length
        }
    }

    getMinimumBufferFixedLengthDataType(): ValueStoreType {
        const fl = this.flags
        if (fl & FieldFlags.NUMERIC_FLOAT) return ValueStoreType.Float64
        if (fl & FieldFlags.NUMERIC_64BIT) return fl & FieldFlags.NUMERIC_SIGNED ? ValueStoreType.Int64 : ValueStoreType.Uint64
        return fl & FieldFlags.NUMERIC_SIGNED ? ValueStoreType.Int32 : ValueStoreType.Uint32
    }

}

type FieldNumber = number
type FieldName = string

export type RecordSchema = {
    fields: Map<FieldName, FieldValueConfig[]>
    lastId: number
    overflowFieldConfigs?: FieldValueConfig[]
}

type Code = string

function generateCodeForEncofingFieldConfig(fieldConfig: FieldValueConfig, fieldName: FieldName, updateFieldConfigFlags: boolean, root: string): Code {
    let code = ''
    switch (fieldConfig.kind) {
        case FieldTag.Boolean:
            code += `if (data === true) {store.writeUint8(0x01)} else if (data === false) {store.writeUint8(0x02)} else throw new Error("not a boolean");`
            break
        case FieldTag.Numeric:
            if (updateFieldConfigFlags && (fieldConfig.flags & FieldFlags.NUMERIC_FLOAT) == 0) {
                if ((fieldConfig.flags & FieldFlags.NUMERIC_SIGNED) == 0) {
                    code += `if (data < 0) throw new Error('Need upgrade, signed detected', data);`
                }
                if ((fieldConfig.flags & FieldFlags.NUMERIC_SIGNED) == 0 && (fieldConfig.flags & FieldFlags.NUMERIC_64BIT) == 0) {
                    code += `if (data > 4294967295) throw new Error('Need upgrade, uint64 detected', data);`
                }
                if ((fieldConfig.flags & FieldFlags.NUMERIC_SIGNED) != 0 && (fieldConfig.flags & FieldFlags.NUMERIC_64BIT) == 0) {
                    code += `if (data < -2147483648  || data > 2147483647) throw new Error('Need upgrade, int64 detected', data);`
                }

                code += `if (!Number.isInteger(data)) throw new Error('Need upgrade, float detected', data);`
            }

            code += `store.writeDouble(data);`
            break
        case FieldTag.NumericArray:
            code += `store.writeVarint32(data.length);`
            code += 'for (const val of data) {'

            if (updateFieldConfigFlags && (fieldConfig.flags & FieldFlags.NUMERIC_FLOAT) == 0) {
                if ((fieldConfig.flags & FieldFlags.NUMERIC_SIGNED) == 0) {
                    code += `if (val < 0) throw new Error('Need upgrade, signed detected', data);`
                }
                if ((fieldConfig.flags & FieldFlags.NUMERIC_SIGNED) == 0 && (fieldConfig.flags & FieldFlags.NUMERIC_64BIT) == 0) {
                    code += `if (val > 4294967295) throw new Error('Need upgrade, uint64 detected', data);`
                }
                if ((fieldConfig.flags & FieldFlags.NUMERIC_SIGNED) != 0 && (fieldConfig.flags & FieldFlags.NUMERIC_64BIT) == 0) {
                    code += `if (val < -2147483648  || val > 2147483647) throw new Error('Need upgrade, int64 detected', data);`
                }

                code += `if (!Number.isInteger(val)) throw new Error('Need upgrade, float detected', data);`
            }
            code += `  store.writeDouble(val);`
            code += '}'
            break
        case FieldTag.String:
            if (updateFieldConfigFlags && (fieldConfig.flags & FieldFlags.STRING_FIX_LEN) != 0) {
                code += `if (data.length !== ${fieldConfig.len}) throw new Error('Need upgrade, variable length', data);`
            }
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
            code += generateEncoderCodeForSchema(fieldConfig.elementSchema as RecordSchema, updateFieldConfigFlags, `${root}.${fieldName}`)
            code += '  }'
            code += '}'
            break
        case FieldTag.ChildArray:
            code += 'store.writeVarint32(data.length);'
            code += 'for (const doc of data) {'
            code += '  if (doc === null) {'
            code += '    store.writeVarint32(-1);'
            code += '  } else {'
            code += generateEncoderCodeForSchema(fieldConfig.elementSchema as RecordSchema, updateFieldConfigFlags, `${root}.${fieldName}`)
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
            code += generateEncoderCodeForSchema(fieldConfig.elementSchema as RecordSchema, updateFieldConfigFlags, `${root}.${fieldName}`)
            code += '          }'
            code += '        }'
            code += '      }'
            code += '    }'
            code += '  } /* end for */'
            code += '} /* end function */'
            code += 'encodeMixedArray(data);'

            break

        default:
            throw new Error(`not yet implemented for ${fieldConfig.kind}`)
    }
    return code
}

function generateEncoderForPrimitiveArrays(primitiveArrayFieldConfigs: FieldValueConfig[], writeFieldName: boolean, fieldName: FieldName,updateFieldConfigFlags: boolean, root: string): Code {
    let code = ''
    if (primitiveArrayFieldConfigs.length == 1) {
        code += `store.writeVarint32(${primitiveArrayFieldConfigs[0].id});`
        if (writeFieldName) code += `store.writeVString(field);`
        code += generateCodeForEncofingFieldConfig(primitiveArrayFieldConfigs[0], fieldName,updateFieldConfigFlags, root)
    } else {
        code += 'if (data.length == 0) {'
        code += `store.writeVarint32(${primitiveArrayFieldConfigs[0].id});`
        if (writeFieldName) code += `store.writeVString(field);`
        code += generateCodeForEncofingFieldConfig(primitiveArrayFieldConfigs[0], fieldName,updateFieldConfigFlags, root)
        for (const pfc of primitiveArrayFieldConfigs) {
            switch (pfc.kind) {
                case FieldTag.NumericArray:
                    code += '} else if (typeof data[0] === "numeric") {'
                    code += `store.writeVarint32(${pfc.id});`
                    if (writeFieldName) code += `store.writeVString(field);`
                    code += generateCodeForEncofingFieldConfig(pfc, fieldName,updateFieldConfigFlags, root)
                    break
                case FieldTag.StringArray:
                    code += '} else if (typeof data[0] === "string") {'
                    code += `store.writeVarint32(${pfc.id});`
                    if (writeFieldName) code += `store.writeVString(field);`
                    code += generateCodeForEncofingFieldConfig(pfc, fieldName,updateFieldConfigFlags, root)
                    break
            }
        }
        code += '}'
    }
    return code
}

function generateEncoderCodeForRecordArray(arrayChildFieldConfig: FieldValueConfig, writeFieldName: boolean, fieldName: FieldName,updateFieldConfigFlags: boolean, root: string): Code {
    let code = `store.writeVarint32(${arrayChildFieldConfig.id});`
    if (writeFieldName) code += `store.writeVString(field);`
    code += generateCodeForEncofingFieldConfig(arrayChildFieldConfig, fieldName,updateFieldConfigFlags, root)
    return code
}

function generateEncoderCodeForChildRecord(childFieldConfig: FieldValueConfig, writeFieldName: boolean, fieldName: FieldName,updateFieldConfigFlags: boolean, root: string): Code {
    let code = `store.writeVarint32(${childFieldConfig.id});`
    if (writeFieldName) code += `store.writeVString(field);`
    code += generateCodeForEncofingFieldConfig(childFieldConfig, fieldName,updateFieldConfigFlags, root)
    return code
}

function generateCodeForEncondingMultiTypeField(fieldConfigs: FieldValueConfig[], fieldName: FieldName,updateFieldConfigFlags: boolean,  root: string, writeFieldName: boolean = false): Code {
    let code = 'switch (typeof data) {'

    const primitiveFieldConfigs = fieldConfigs.filter(f => f.kind == FieldTag.Boolean || f.kind == FieldTag.String || f.kind == FieldTag.Numeric)

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
        code += generateCodeForEncofingFieldConfig(fieldConfig, fieldName,updateFieldConfigFlags, root)
        code += 'break; }'
    }

    const childFieldConfig = fieldConfigs.find(f => f.kind == FieldTag.Child)
    const arrayChildFieldConfig = fieldConfigs.find(f => f.kind == FieldTag.ChildArray)
    const primitiveArrayFieldConfigs = fieldConfigs.filter(f => f.kind == FieldTag.StringArray || f.kind == FieldTag.NumericArray)

    const mixedArrayFieldConfig: FieldValueConfig | undefined = fieldConfigs.find(f => f.kind == FieldTag.MixedArray)
    const bufferFieldConfigs = fieldConfigs.filter(f => f.kind == FieldTag.BufferValue)
    if (bufferFieldConfigs.length > 0) throw new Error('Not yet implemented')

    const hasPrimitiveArrayConfig = primitiveArrayFieldConfigs.length > 0

    if (childFieldConfig || hasPrimitiveArrayConfig || arrayChildFieldConfig || mixedArrayFieldConfig) {
        code += "case 'object': {"

        code += 'if (Array.isArray(data)) {'

        if (mixedArrayFieldConfig) {
            code += `store.writeVarint32(${mixedArrayFieldConfig.id});`
            if (writeFieldName) code += `store.writeVString(field);`
            code += generateCodeForEncofingFieldConfig(mixedArrayFieldConfig, fieldName,updateFieldConfigFlags, root)
        } else {
            code += '  if (data.length === 0) {'
            if (hasPrimitiveArrayConfig) {
                code += generateEncoderForPrimitiveArrays(primitiveArrayFieldConfigs, writeFieldName, fieldName,updateFieldConfigFlags, root)
            } else if (arrayChildFieldConfig) {
                code += generateEncoderCodeForRecordArray(arrayChildFieldConfig, writeFieldName, fieldName,updateFieldConfigFlags, root)
            } else {
                code += "throw new Error('array types not yet supported')"
            }
            code += "  } else if (typeof data[0] === 'number' || typeof data[0] === 'string') {"
            if (hasPrimitiveArrayConfig) {
                code += generateEncoderForPrimitiveArrays(primitiveArrayFieldConfigs, writeFieldName, fieldName,updateFieldConfigFlags, root)
            } else {
                code += "throw new Error('primitive array type not yet supported')"
            }
            code += "  } else if (typeof data[0] === 'object') {"
            if (arrayChildFieldConfig) {
                code += generateEncoderCodeForRecordArray(arrayChildFieldConfig, writeFieldName, fieldName,updateFieldConfigFlags, root)
            } else {
                code += "throw new Error('record array type not yet supported')"
            }
            code += '  } else {'
            code += "    throw new Error('bug')"
            code += '  }'
        }
        code += "} else if (typeof data === 'object') {"
        if (childFieldConfig) {
            code += generateEncoderCodeForChildRecord(childFieldConfig, writeFieldName, fieldName,updateFieldConfigFlags, root)
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

function generateEncoderCodeForSchema(type: RecordSchema, updateFieldConfigFlags: boolean, root = ''): Code {
    let code = `for (const [field, data] of Object.entries(doc)) {`

    code += ' switch (field) {'
    for (const [fieldName, fieldConfigs] of type.fields.entries()) {
        code += `case "${fieldName}": {`

        if (fieldConfigs.length == 1) {
            const fieldConfig = fieldConfigs[0]
            code += `store.writeVarint32(${fieldConfig.id});`
            code += generateCodeForEncofingFieldConfig(fieldConfig, fieldName,updateFieldConfigFlags, root)
        } else {
            code += generateCodeForEncondingMultiTypeField(fieldConfigs, fieldName,updateFieldConfigFlags, root)
        }

        code += ' break; } '
    }
    if (type.overflowFieldConfigs) {
        code += ' default:'
        const fieldConfigs = type.overflowFieldConfigs

        if (fieldConfigs.length == 1) {
            const fieldConfig = fieldConfigs[0]
            code += `store.writeVarint32(${fieldConfig.id});`
            code += `store.writeVString(field);`
            code += generateCodeForEncofingFieldConfig(fieldConfig, '£_mapped',updateFieldConfigFlags, root)
        } else {
            code += generateCodeForEncondingMultiTypeField(fieldConfigs, '£_mapped',updateFieldConfigFlags, root, true)
        }
    } else {
        code += `  default: throw new Error('failure with field ${root}.' + field)`
    }
    code += '}'

    code += '}'
    code += 'store.writeVarint32(0x00);\n'

    return code
}

function generateCodeForDecodingField(fieldConfig: FieldValueConfig, fieldName: string | null): Code {
    let code = `case ${fieldConfig.id}: {`

    let fieldIndex
    if (fieldName === null) {
        code += 'const nextFieldIndex = store.readVString(pointer); pointer+=nextFieldIndex.length;'
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
            code += 'const lenRead = store.readVarint32(pointer); pointer+=lenRead.length;'
            code += 'const len = lenRead.value;'
            code += 'const array = [];'
            code += 'for (let i = 0; i < len; i++) {'
            code += 'const nextRead = store.readDouble(pointer); pointer+=8;'
            code += 'array[i]=nextRead;'
            code += '}'
            code += `result[${fieldIndex}] = array;`
            break
        case FieldTag.String:
            code += 'const nextRead = store.readVString(pointer); pointer+=nextRead.length;'
            code += `result[${fieldIndex}] = nextRead.string;`
            break
        case FieldTag.StringArray:
            code += 'const lenRead = store.readVarint32(pointer); pointer+=lenRead.length;'
            code += 'const len = lenRead.value;'
            code += 'const array = [];'
            code += 'for (let i = 0; i < len; i++) {'
            code += 'const nextRead = store.readVString(pointer); pointer+=nextRead.length;'
            code += 'array[i]=nextRead.string;'
            code += '}'
            code += `result[${fieldIndex}] = array;`
            break
        case FieldTag.Child:
            code += 'const parent = result; {'
            code += generateDecoderCodeForType(fieldConfig.elementSchema as RecordSchema)
            code += `parent[${fieldIndex}] = result; }`
            break
        case FieldTag.ChildArray:
            code += 'const parent = result; {'
            code += 'const lenRead = store.readVarint32(pointer); pointer+=lenRead.length;'
            code += 'const len = lenRead.value;'
            code += 'const array = [];'
            code += 'for (let i = 0; i < len; i++) {'
            code += generateDecoderCodeForType(fieldConfig.elementSchema as RecordSchema)
            code += 'array[i]=result;'
            code += '}'
            code += `parent[${fieldIndex}] = array; }`
            break
        case FieldTag.MixedArray:
            code += 'function decodeMixedArray() {'

            code += 'const lenRead = store.readVarint32(pointer); pointer+=lenRead.length;'
            code += 'const len = lenRead.value;'
            code += 'const array = [];'

            code += 'for (let i = 0; i < len; i++) {'

            code += '  const elTypeRead = store.readVarint32(pointer); pointer+=elTypeRead.length;'
            code += '  const elType = elTypeRead.value;'
            code += '  switch (elType) {'
            code += `    case (${FieldTag.Numeric}): {`
            code += '      const nextRead = store.readDouble(pointer); pointer+=8;'
            code += '      array[i] = nextRead;'
            code += '      break;'
            code += '    }'
            code += `    case (${FieldTag.String}): {`
            code += '      const nextRead = store.readVString(pointer); pointer+=nextRead.length;'
            code += '      array[i] = nextRead.string;'
            code += '      break;'
            code += '    }'
            code += `    case (${FieldTag.Child}): {`
            code += '      const parent = array; {'
            code += generateDecoderCodeForType(fieldConfig.elementSchema as RecordSchema)

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

function generateDecoderCodeForType(type: RecordSchema): Code {
    let code = ''
    code += `let result = {};`
    code += `const nextRead = store.readVarint32(pointer); pointer+=nextRead.length; let fieldId=nextRead.value; `
    code += 'if (fieldId === -1) {'
    code += '   result = null'
    code += '} else {'

    code += `while (fieldId !== 0x00) {`

    code += ' switch (fieldId) {'
    for (const [fieldName, fieldConfigs] of type.fields.entries()) {
        for (const fieldConfig of fieldConfigs) {
            code += generateCodeForDecodingField(fieldConfig, fieldName)
        }
        if (type.overflowFieldConfigs) {
            for (const fieldConfig of type.overflowFieldConfigs) {
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

function generateEncoder(type: RecordSchema, updateFieldConfigFlags: boolean): GeneratedEncoder {
    const code = generateEncoderCodeForSchema(type, updateFieldConfigFlags)

    try {
        return {
            function: new Function('doc', 'store', code) as (doc: Doc, store: ByteBuffer) => void,
            code: code
        }
    } catch (e) {
        console.error('Issue in generated code for encoder:', code)
        throw e
    }
}

export type GeneratedDecoder = {
    function: (pointer: number, store: ByteBuffer) => Doc
    code: string
}

export function generateDecoder(type: RecordSchema): GeneratedDecoder {
    let code = generateDecoderCodeForType(type)
    code += 'return result;'

    try {
        return {
            function: new Function('pointer', 'store', code) as (pointer: number, store: ByteBuffer) => Doc,
            code: code
        }
    } catch (e) {
        console.error('Issue in generated code for decoder:', code)
        throw e
    }
}

function upgradeSchema(doc: object, current: RecordSchema, targetMaxFieldTagsPerLevel: number, updateFieldConfigFlags: boolean): void {
    if (!doc) {
        throw new Error()
    }
    for (const [field, data] of Object.entries(doc)) {
        let fieldConfigs = current.fields.get(field)

        if (!fieldConfigs && current.overflowFieldConfigs) {
            fieldConfigs = current.overflowFieldConfigs
        }

        let fieldConfig
        let neededKind

        switch (typeof data) {
            case 'boolean':
                neededKind = FieldTag.Boolean
                fieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.Boolean)
                break
            case 'number':
                neededKind = FieldTag.Numeric
                fieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.Numeric)
                break
            case 'string':
                neededKind = FieldTag.String
                fieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.String)
                break
            case 'object':
                if (Array.isArray(data)) {
                    // once an array with mixed was needed, all future encode must be done with mixed format
                    const mixedArrayConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.MixedArray)

                    if (mixedArrayConfig) {
                        neededKind = FieldTag.MixedArray
                        fieldConfig = mixedArrayConfig
                    } else {
                        const typesInArray = data.reduce((acc, el) => {
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
                                    fieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.NumericArray)
                                    break
                                case 'string':
                                    neededKind = FieldTag.StringArray
                                    fieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.StringArray)
                                    break
                                case 'object':
                                    neededKind = FieldTag.ChildArray
                                    fieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.ChildArray)
                                    break
                                default:
                                    throw new Error('not yet implemented')
                            }
                        } else {
                            neededKind = FieldTag.MixedArray
                        }
                    }
                } else if (Buffer.isBuffer(data)) {
                    neededKind = FieldTag.BufferValue
                    fieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.BufferValue)
                } else {
                    neededKind = FieldTag.Child
                    fieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.Child)
                }
        }

        if (!fieldConfig && neededKind !== undefined) {
            if (!fieldConfigs) {
                if (current.lastId >= targetMaxFieldTagsPerLevel) {
                    current.overflowFieldConfigs = fieldConfigs = new Array<FieldValueConfig>()
                } else {
                    fieldConfigs = new Array<FieldValueConfig>()
                    current.fields.set(field, fieldConfigs)
                }
            }

            current.lastId++
            let childSchema

            if (neededKind == FieldTag.Child || neededKind == FieldTag.ChildArray) {
                childSchema = {
                    fields: new Map<FieldName, FieldValueConfig[]>(),
                    lastId: 0
                }

                if (Array.isArray(data)) {
                    for (const el of data) {
                        if (el !== null) upgradeSchema(el, childSchema, targetMaxFieldTagsPerLevel, updateFieldConfigFlags)
                    }
                } else if (data !== null) {
                    upgradeSchema(data, childSchema, targetMaxFieldTagsPerLevel, updateFieldConfigFlags)
                }
            } else if (neededKind == FieldTag.MixedArray) {
                const childArrayFieldConfig = fieldConfigs?.find(fc => fc.kind == FieldTag.ChildArray)

                // if there was previous a config for arrays of records, reuse the yet existing schema
                if (childArrayFieldConfig) {
                    childSchema = childArrayFieldConfig.elementSchema as RecordSchema
                } else {
                    childSchema = {
                        fields: new Map<FieldName, FieldValueConfig[]>(),
                        lastId: 0
                    }
                }

                function traverseArray(array: [], type: RecordSchema): void {
                    for (const el of array) {
                        if (el !== null && typeof el === 'object') {
                            if (Array.isArray(el)) {
                                traverseArray(el, type)
                            } else {
                                upgradeSchema(el, type, targetMaxFieldTagsPerLevel, updateFieldConfigFlags)
                            }
                        }
                    }
                }

                traverseArray(data, childSchema)
            }

            fieldConfig = new FieldValueConfig(current.lastId, neededKind, childSchema)
            fieldConfigs.push(fieldConfig)
        } else if (fieldConfig?.elementSchema) {
            if (Array.isArray(data)) {
                for (const el of data) {
                    if (el !== null) upgradeSchema(el, fieldConfig.elementSchema, targetMaxFieldTagsPerLevel, updateFieldConfigFlags)
                }
            } else if (data !== null) {
                upgradeSchema(data, fieldConfig.elementSchema, targetMaxFieldTagsPerLevel, updateFieldConfigFlags)
            }
        }

        if (updateFieldConfigFlags) fieldConfig?.updateFlags(data);
    }
}




export class DocPackedArray implements ValueWithMemoryEstimation {
    readonly pointers: RoaringBitmap32
    readonly store: ByteBufferAccess
    readonly targetMaxFieldTagsPerLevel: number

    readonly rootSchema: RecordSchema

    private encoder: GeneratedEncoder | undefined
    decoder: GeneratedDecoder | undefined
    private updateFieldConfigFlags: boolean

    private constructor(store: ByteBufferAccess, pointers: RoaringBitmap32, targetMaxFieldTagsPerLevel: number, rootSchema: RecordSchema, updateFieldConfigFlags: boolean = false) {
        this.targetMaxFieldTagsPerLevel = targetMaxFieldTagsPerLevel
        this.store = store
        this.pointers = pointers
        this.rootSchema = rootSchema
        this.encoder = generateEncoder(this.rootSchema, updateFieldConfigFlags)
        this.decoder = generateDecoder(this.rootSchema)
        this.updateFieldConfigFlags = updateFieldConfigFlags

    }

    static createNew(targetMaxFieldTagsPerLevel: number = 127) {
        const pageSize = 4 * 1024
        const buffer = ByteBuffer.allocate(pageSize)

        return new DocPackedArray(() => buffer, new RoaringBitmap32(), targetMaxFieldTagsPerLevel, {
            fields: new Map(),
            lastId: 0
        })
    }

    static load(source: ByteBufferAccess, pointers: RoaringBitmap32, rootSchema: RecordSchema) {
        return new DocPackedArray(source, pointers, 127, rootSchema)
    }

    add(doc: Doc): void {
        const store = this.store()
        try {
            store.mark()

            if (!this.encoder) throw Error('not yet generated')

            this.encoder.function(doc, store)

            // BB do not extend the limit when expending the capacity
            store.limit = store.capacity()
        } catch (e) {
            store.reset()
            upgradeSchema(doc, this.rootSchema, this.targetMaxFieldTagsPerLevel, this.updateFieldConfigFlags)
            const prevEncode = this.encoder?.code

            this.encoder = generateEncoder(this.rootSchema, this.updateFieldConfigFlags)
            this.decoder = generateDecoder(this.rootSchema)

            try {
                store.mark()
                this.encoder.function(doc, store)

                // BB do not extend the limit when expending the capacity
                store.limit = store.capacity()
            } catch (e) {
                store.reset()
                throw new Error(
                    `Maybe issue in generated code:\n${e}\n\ncode:\n${this.encoder.code}\n\nprev_code:\n${prevEncode}\n\nobject:\n${util.inspect(
                        doc,
                        false,
                        null,
                        true
                    )}`
                )
            }
        }

        this.pointers.add(store.offset)
    }

    get(index: number): Doc | undefined {
        const pointer = this.pointers.select(index - 1) || 0

        return this.decoder?.function(pointer, this.store())
    }

    *iterateBuffers() {
        let from = 0
        const buffer = this.store().buffer
        for (const to of this.pointers) {
            yield buffer.slice(from, to)
            from = to
        }
    }

    *iterateObjects() {
        let from = 0
        const buffer = this.store()
        for (const to of this.pointers) {
            yield this.decoder?.function(from, buffer) as Doc
            from = to
        }
    }


    get length(): number {
        return this.pointers.size
    }

    addAll(docs: Doc[]) {
        const store = this.store()
        let i = 0;
        try {
            for (; i < docs.length; i++) {
                const doc = docs[i]

                store.mark()
                if (!this.encoder) throw Error('not yet generated')

                this.encoder.function(doc, store)
                this.pointers.add(store.offset)
            }
            // BB do not extend the limit when expending the capacity
            store.limit = store.capacity()
        } catch (e) {
            store.reset()

            for (let j = i; j < docs.length; j++) {
                upgradeSchema(docs[j], this.rootSchema, this.targetMaxFieldTagsPerLevel, this.updateFieldConfigFlags)
            }

            const prevEncode = this.encoder?.code

            this.encoder = generateEncoder(this.rootSchema, this.updateFieldConfigFlags)
            this.decoder = generateDecoder(this.rootSchema)

            store.mark()
            const markPointer = this.pointers.maximum()

            let doc
            try {
                for (; i < docs.length; i++) {
                    doc = docs[i]

                    this.encoder.function(doc, store)
                    this.pointers.add(store.offset)
                }
                // BB do not extend the limit when expending the capacity
            } catch (e) {
                store.reset()
                this.pointers.removeRange(markPointer+1, Number.MAX_SAFE_INTEGER)
                throw new Error(
                    `Maybe issue in generated code:\n${e}\n\ncode:\n${this.encoder.code}\n\nprev_code:\n${prevEncode}\n\nobject:\n${util.inspect(
                        doc,
                        false,
                        null,
                        true
                    )}`
                )
            } finally {
                store.limit = store.capacity()
            }
        }



    }

    get sizeInMemory() {
        return this.store().buffer.length + this.pointers.getSerializationSizeInBytes()
    }
}
