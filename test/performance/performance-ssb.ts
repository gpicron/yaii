import anyTest, {TestInterface} from 'ava'
import {MemoryInvertedIndex} from '../../src'
import {FieldConfigFlag, Doc, and, or, present, ResultItem, SortDirection, token, mooTokenizer} from '../../src/yaii-types'
import {Source} from 'pull-stream'
import {AboutContent, ContactContent, Content, FeedId, Msg} from 'ssb-typescript/readme'
import {as, count, first, from, reduce} from 'ix/asynciterable'
import * as op from 'ix/asynciterable/operators'
import {PullSourceAsyncIterator} from './pull-source-async-iterator'


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
            'value.timestamp': {
                flags: FieldConfigFlag.SORT_OPTIMIZED | FieldConfigFlag.SEARCHABLE,
                addToAllField: false,
                analyzer: undefined,
            },
            'value.hash': {
                flags: 0,
                addToAllField: false,
                analyzer: undefined
            },
            'TDA' : {
                flags: FieldConfigFlag.SORT_OPTIMIZED,
                addToAllField: false,
                analyzer: undefined,
                generator: (input: Doc) => {
                    const msg = input as Msg<Content>

                    const authorTS = msg.value.timestamp || Number.MAX_SAFE_INTEGER

                    return Math.min(Math.floor(msg.timestamp), Math.floor(authorTS))
                }
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
        op.take(5000),
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
    // look for all 'about' messages from one identity, gather last names

    const profileId = await t.context.server.whoami().id

    const query = index
        .query<Msg<AboutContent>>(and(
            token('about', 'value.content.type'),
            token(profileId, 'value.author'),
            token(profileId, 'value.content.about')
        ), [
            {field:"TDA", dir: SortDirection.DESCENDING}
        ])
        .pipe(
            op.tap(x => log(x)),
            op.map(x  => x._source),
        )


    const returnedResult = await reduce(query, (previousValue: string[], currentValue: Msg<AboutContent> | undefined, currentIndex) => {
            const name =  currentValue?.value.content.name
            if (name) {
                previousValue.unshift(name)
            }
            return previousValue
        }, [])


    console.log("last names used: ", returnedResult)

    console.log('--------------------------------------------------')
    // count msgs group by type where the profile id appears in any field

    const allReferencing = index.query<Msg<Content>>(and(
        present('value.content.type'),
        token(profileId)
    ))

    // @ts-ignore
    const returnedResult2 = await allReferencing.pipe(
        op.catchError(error => {
            // @ts-ignore
            log(index.segment.docSourceStore.decoder.code)
            throw error
        }),
        op.groupBy(
            (doc: ResultItem<Msg<Content>>) => {
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
    // building a social network data structure from me with classic async coding way demo

    const identities = new Map<FeedId, Network>()

    const returnedResult3 =
        index.query<Msg<ContactContent>>(
                and(
                    token('contact', 'value.content.type'),
                    token(profileId, 'value.author'),
                )
            )


    for await (const result of returnedResult3) {
        const msg = result._source as Msg<ContactContent>
        log(msg)
        const author = msg.value.author
        const contact = msg.value.content.contact
        const following = msg.value.content.following
        const blocking = msg.value.content.blocking

        if (contact) {

            let identity= identities.get(author)
            if (!identity) {
                identity = {} as Network
                identities.set(author, identity)
            }

            let identityRel = identity[contact]
            if (!identityRel) {
                identityRel = {
                    lastUpdate: Number.MIN_SAFE_INTEGER,
                    status: "neutral"
                }
                identity[contact] = identityRel
            }

            if (msg.value.timestamp > identityRel.lastUpdate) {
                identityRel.lastUpdate = msg.value.timestamp
                if (following === true) {
                    identityRel.status = "following"
                } else if (blocking === true) {
                    identityRel.status = "blocking"
                } else if (following === false || blocking === false) {
                    identityRel.status = "neutral"
                }
            }


        }
    }

    log(identities.get(profileId))
    log(identities)


    console.log('--------------------------------------------------')

    // a more efficient approach than recreating a new structure to hold reduced states while perfectly viable performance wise

    async function getName(id: FeedId) {
        const iter = index.query<Msg<AboutContent>>(and(
                token('about', 'value.content.type'),
                token(id, 'value.author'),
                token(id, 'value.content.about'),
                present('value.content.name')
            ), [
                {field:"TDA", dir: SortDirection.DESCENDING}
            ], 1)[Symbol.asyncIterator]()

        return iter.next().then(r => r.value?._source.value?.content?.name)
    }

    async function* getFollowed(id: FeedId) {
        const iter = index.query<Msg<ContactContent>>(and(
            token('contact', 'value.content.type'),
            token(id, 'value.author'),
            or(present('value.content.following'), present('value.content.blocking'))
        ),
            [{field:'value.timestamp', dir:SortDirection.DESCENDING}]
        ).pipe(
            op.tap(log),
            op.map(r => r._source as Msg<ContactContent>),
            op.groupBy(
                msg => msg.value.content.contact,
                msg => msg.value
                )
        )

        for await (const result of iter) {
            const lastUpdate = await first(result)
            if (result.key && lastUpdate?.content.following === true) {
                yield result.key
            }
        }
    }


    log(await getName(profileId))
    for await (const followed of getFollowed(profileId)) {
        const name = await getName(followed)
        log(`|-> follows '${name} (${followed})`)
    }



    t.pass()

})

type RelationStatus = {
    lastUpdate: number,
    status: "following" | "blocking" | "neutral"
}

type Network = Record<FeedId, RelationStatus>

