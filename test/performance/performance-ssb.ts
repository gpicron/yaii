import anyTest, { TestInterface } from 'ava'
import { MemoryInvertedIndex } from '../../src'
import {
    and,
    FieldConfigFlag,
    present,
    ResultItem,
    token
} from '../../src/yaii-types'
import { Source } from 'pull-stream'
import { Content, Msg } from 'ssb-typescript/readme'
import { as, count, from } from 'ix/asynciterable'
import * as op from 'ix/asynciterable/operators'
import { PullSourceAsyncIterator } from './pull-source-async-iterator'
import { mooTokenizer } from '../../src/lib/analyzer/moo-tokenizer'
import pify = require('pify')

const pull = require('pull-stream')
const flotilla = require('@fraction/flotilla')

const SecretStack = require('secret-stack')
const Config = require('ssb-config/inject')
const ssbClient = require('ssb-client')
const ssbKeys = require('ssb-keys')
const util = require('util')

interface TestContext {
    server: SSBServer
}

interface SSBServer {
    close: Function
    config: {
        path: string
    }
    whoami: Function
    createLogStream: (opts: unknown) => Source<Msg>
    createHistoryStream: (opts: unknown) => Source<Msg>
    ready: Function
    status: Function
}

const test = anyTest as TestInterface<TestContext>

test.before(async t => {
    const config = Config()

    const serverFactory = SecretStack(config)
        .use(require('ssb-db'))
        .use(require('ssb-master'))
        .use(require('ssb-backlinks'))

    const server = flotilla(config)
    //const server = serverFactory(config) // start application
    server.whoami((err: unknown, res: unknown) => {
        if (err) throw err
        console.log(res)
    })

    config.manifest = server.getManifest()

    const keys = await pify(ssbKeys.load)(`${config.path}/secret`)

    t.context = {
        server: server
    }

    return
})

test.after.always(async t => {
    return await pify(t.context.server.close)()
})

const indexedTypes: Record<
    string,
    boolean | ((msg: Msg, content: any) => Array<any>)
> = {
    about: true,
    pub: true,
    contact: true,
    post: true,
    vote: true,
    channel: true,
    'git-repo': true,
    'git-update': true,
    issue: true,
    'issue-edit': true,
    'pull-request': true,
    flag: true,
    'ssb-dns': true,
    'npm-packages': true
}

const mainAnalyzer = mooTokenizer({
    WS: { match: /[\n\s,.:;"']+/, lineBreaks: true },
    TOKEN: [
        { match: /[@%&][A-Za-z0-9\/+]{43}=\.[\w\d]+/ }, // extracts refs
        { match: /[^\n\s,.:;"']+/, value: x => x.toLowerCase() }
    ]
})

function printMemoryUsage() {
    if (global.gc) {
        global.gc()
    } else {
        console.warn(
            'No GC hook! Start your program as `node --expose-gc file.js`.'
        )
    }

    const used = process.memoryUsage()
    for (let key in used) {
        console.log(
            // @ts-ignore
            `${key} ${Math.round((used[key] / 1024 / 1024) * 100) / 100} MB`
        )
    }
}

function log(x: any) {
    console.log(util.inspect(x, false, null, true))
}

test('Simple test of indexing', async t => {
    printMemoryUsage()

    const index = new MemoryInvertedIndex(
        {
            'value.author': {
                flags: FieldConfigFlag.SEARCHABLE,
                addToAllField: true,
                analyzer: undefined
            },
            'value.content.about': {
                flags: FieldConfigFlag.SEARCHABLE,
                addToAllField: true,
                analyzer: undefined
            },
            'value.sequence': {
                flags: 0,
                addToAllField: false,
                analyzer: undefined
            },
            'value.signature': {
                flags: 0,
                addToAllField: false,
                analyzer: undefined
            },
            'value.hash': {
                flags: 0,
                addToAllField: false,
                analyzer: undefined
            }
        },
        {
            storeSourceDoc: true,
            defaultFieldConfig: {
                addToAllField: true,
                flags: FieldConfigFlag.SEARCHABLE,
                analyzer: mainAnalyzer
            },
            allFieldConfig: {
                flags: FieldConfigFlag.SEARCHABLE,
                analyzer: mainAnalyzer
            }
        }
    )

    t.log(t.context.server.ready())

    const source = t.context.server.createLogStream({
        old: true,
        live: false,
        sync: false,
        gt: 0
    })

    let counter = 1

    let start = new Date()
    const startAll = start

    const filterTypes = (msg: Msg) => {
        const type = msg.value.content?.type
        // @ts-ignore
        return type && indexedTypes[type] ? true : false
    }

    const messages = from(new PullSourceAsyncIterator(source)).pipe(
        op.filter(filterTypes),
        op.map((value, index) => {
            if (index % 30000 == 0) {
                const end = new Date()
                console.log(
                    'progress:',
                    index,
                    '\t',
                    (end.getTime() - start.getTime()) / 1000,
                    'seconds'
                )
                start = end
            }
            return value
        })
    )

    // @ts-ignore
    const indexed = await index.add(messages)

    const end = new Date()
    console.log('indexed:' + indexed)
    console.log(
        'total duration',
        '\t',
        (end.getTime() - startAll.getTime()) / 1000,
        'seconds'
    )
    printMemoryUsage()

    //console.log(index.listAllKnownField())

    console.log('--------------------------------------------------')
    const returnedResult = await count(
        index
            .query(
                and(
                    token('about', 'value.content.type'),
                    token(
                        '@gBZQVjIukvbX8Bs22vdTfAHMiVfE9nR+NvXQYVaqeIg=.ed25519',
                        'value.author'
                    )
                )
            )
            .pipe(op.tap(x => log(x)))
    )

    console.log('--------------------------------------------------')

    const allReferencing = index.query(
        and(
            present('value.content.type'),
            token('@gBZQVjIukvbX8Bs22vdTfAHMiVfE9nR+NvXQYVaqeIg=.ed25519')
        )
    )

    // @ts-ignore
    const returnedResult2 = await allReferencing.pipe(
        op.catchError(error => {
            // @ts-ignore
            log(index.segment.docSourceStore.decoder.code)
            throw error
        }),
        op.groupBy(
            (doc: ResultItem) => {
                // @ts-ignore
                const msg: Msg<Content> = doc._source as Msg<Content>
                if (msg.value.content?.type) {
                    return msg.value.content?.type
                } else {
                    console.error(doc)
                    t.fail()
                }
            },
            doc => doc._source?.key,
            async (key, values) => [key, await count(as(values))]
        )
    )

    for await (const x of returnedResult2) {
        log(x)
    }

    console.log('--------------------------------------------------')

    const returnedResult3 = await count(
        index.query(and(token('borric'))).pipe(op.tap(d => log(d)))
    )

    t.true(returnedResult3 > 0)
})
