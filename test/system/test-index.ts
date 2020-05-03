import anyTest, { TestInterface } from 'ava'
import { MemoryInvertedIndex } from '../../src'
import {
    all,
    and,
    Doc,
    FieldConfigFlag,
    number,
    numberRange,
    or,
    QueryOperator,
    ResultItem,
    SortClause,
    SortDirection,
    token
} from '../../src/yaii-types'
import { as, toArray, toSet } from 'ix/asynciterable'
import * as op from 'ix/asynciterable/operators'

interface TestContext {
    index: MemoryInvertedIndex
}

const test = anyTest as TestInterface<TestContext>

test.before(async t => {
    t.context.index = await createIndex()
})

test('Simple test of indexing', async t => {
    const index = new MemoryInvertedIndex({
        id: { flags: FieldConfigFlag.STORED },
        flag: { flags: FieldConfigFlag.SEARCHABLE }
    })

    await index.add({
        id: '1234',
        flag: 'true'
    })

    t.is(index.size, 1)

    await index.add({
        id: '1235',
        flag: 23
    })

    t.is(index.size, 2)

    await index.add(
        as([
            {
                id: '1237',
                flag: 23
            },
            {
                id: '1238',
                flag: 23
            }
        ])
    )

    t.is(index.size, 4)

    const allDocsId = await toSet(
        index
            .query(
                {
                    operator: QueryOperator.ALL
                },
                ['id']
            )
            .pipe(op.map(value => value.id))
    )

    // @ts-ignore
    t.deepEqual(allDocsId, new Set(['1234', '1235', '1237', '1238']))
})

const docs = [
    {
        id: '12',
        text: 'lorem ipsum',
        token_data: 'abc',
        number_data: 20,
        unknown_data: 'ufg about test'
    },
    {
        id: '13',
        text: 'dolor',
        token_data: 'efg',
        number_data: 30
    },
    {
        id: '14',
        text: 'this is a demo',
        token_data: ['abc', 'bcd'],
        number_data: 25
    },
    {
        id: '15',
        text: 'and it is working',
        token_data: 'hij',
        number_data: 10000000
    }
]

async function createIndex() {
    const index = new MemoryInvertedIndex({
        id: { flags: FieldConfigFlag.STORED },
        text: { flags: FieldConfigFlag.SEARCHABLE, addToAllField: false },
        token_data: { flags: FieldConfigFlag.SEARCHABLE },
        number_data: { flags: FieldConfigFlag.SEARCHABLE }
    })

    await index.add(as(docs as Array<Doc>))
    return index
}

// @ts-ignore
async function assertQuery(
    t,
    query,
    expected,
    querySort?: Array<SortClause>,
    limit = 1000
) {
    let allDocsId = await toArray(
        (t.context.index as MemoryInvertedIndex)
            .query(query, ['id'], limit, querySort)
            .pipe(op.map(value => value.id))
    )
    if (!querySort) {
        allDocsId = allDocsId.sort()
        expected = expected.sort()
    }

    // @ts-ignore
    t.deepEqual(allDocsId, expected)
}

test('Test Token query', async t => {
    t.is(t.context.index.size, 4)

    await assertQuery(t, token('abc', 'token_data'), ['12', '14'])
})

test('Test Or query', async t => {
    await assertQuery(
        t,
        or(token('abc', 'token_data'), token('efg', 'token_data')),
        ['12', '13', '14']
    )
    await assertQuery(
        t,
        or(token('abc', 'token_data'), token('bcs', 'token_data')),
        ['12', '14']
    )
})

test('Test And query', async t => {
    await assertQuery(
        t,
        and(token('abc', 'token_data'), token('efg', 'token_data')),
        []
    )
    await assertQuery(
        t,
        and(token('abc', 'token_data'), token('bcd', 'token_data')),
        ['14']
    )
})

test('Test Number query', async t => {
    await assertQuery(t, number(2000, 'number_data'), [])
    for (const d of docs)
        await assertQuery(t, number(d.number_data, 'number_data'), [d.id])
})

test('Test NumberRange query', async t => {
    await assertQuery(t, numberRange('number_data', 20, 30, false, true), [
        '13',
        '14'
    ])
    await assertQuery(t, numberRange('number_data', 20, 30, true, true), [
        '12',
        '13',
        '14'
    ])
    await assertQuery(t, numberRange('number_data', 20, 30, true, false), [
        '12',
        '14'
    ])
    await assertQuery(t, numberRange('number_data', 20, 26), ['12', '14'])

    await assertQuery(t, numberRange('number_data', 30, 10000000, true, true), [
        '13',
        '15'
    ])
    await assertQuery(
        t,
        numberRange('number_data', 30, Number.MAX_SAFE_INTEGER, true, true),
        ['13', '15']
    )
    await assertQuery(
        t,
        numberRange('number_data', Number.MIN_SAFE_INTEGER, 30, true, false),
        ['12', '14']
    )
})

test('Test NumberRange query with inf bounds', async t => {
    await assertQuery(
        t,
        numberRange('number_data', Number.NEGATIVE_INFINITY, 30, true, false),
        ['12', '14']
    )
    await assertQuery(
        t,
        numberRange('number_data', 30, Number.POSITIVE_INFINITY, true, true),
        ['13', '15']
    )
})

test('Test And of Or query', async t => {
    await assertQuery(
        t,
        and(or(token('abc', 'token_data'), token('efg', 'token_data'))),
        ['12', '13', '14']
    )
    await assertQuery(
        t,
        and(
            or(token('abc', 'token_data'), token('efg', 'token_data')),
            token('abc', 'token_data')
        ),
        ['12', '14']
    )
})

test('Test Sort query', async t => {
    await assertQuery(
        t,
        numberRange('number_data', 20, 30, true, true),
        ['12', '13'],
        [{ field: 'id', dir: SortDirection.ASCENDING }],
        2
    )
    await assertQuery(
        t,
        numberRange('number_data', 20, 30, true, true),
        ['14', '13', '12'],
        [{ field: 'id', dir: SortDirection.DESCENDING }]
    )
    await assertQuery(
        t,
        numberRange('number_data', 20, 30, true, true),
        ['12', '13', '14'],
        [{ field: 'id', dir: SortDirection.ASCENDING }]
    )
})

test('Test Sort query on sort optimized', async t => {
    await assertQuery(
        t,
        numberRange('number_data', 20, 30, true, true),
        ['12', '13'],
        [{ field: 'id', dir: SortDirection.ASCENDING }],
        2
    )
    await assertQuery(
        t,
        numberRange('number_data', 20, 30, true, true),
        ['14', '13', '12'],
        [{ field: 'id', dir: SortDirection.DESCENDING }]
    )
    await assertQuery(
        t,
        numberRange('number_data', 20, 30, true, true),
        ['12', '13', '14'],
        [{ field: 'id', dir: SortDirection.ASCENDING }]
    )
})

test('Test Query on default field config', async t => {
    await assertQuery(t, token('test', 'unknown_data'), ['12'])
})

test('Test Query on _all field config, field configured for not to all', async t => {
    await assertQuery(t, token('lorem', '_all'), [])
})

test('Test Query on _all field config, default config of all use stop words filter', async t => {
    await assertQuery(t, token('about', '_all'), [])
    //await assertQuery(t, token("unknown_data", "about"), ["12"])
    //await assertQuery(t, token("_all", "ufg"), ["12"])
})

test('Test is _source present', async t => {
    const data: ResultItem[] = await toArray(t.context.index.query(all()))

    t.is(data.length, 4)
    data.sort((a: any, b: any) =>
        (a._source.id[0] as string).localeCompare(b._source.id[0])
    )
    for (let i = 0; i < docs.length; i++) {
        // @ts-ignore
        const resultDoc: any = data[i]

        t.deepEqual(resultDoc._source, docs[i])
    }

    t.pass()
})
