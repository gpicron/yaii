import test from 'ava'
import {
    addRange,
    BooleanExpression,
    rangeToExp,
    TermExp
} from '../../src/lib/query-ir'
import { range, toArray } from 'ix/iterable'

test('numeric range exp level 0', t => {
    const exp = rangeToExp('f', BigInt(25), 0, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term.toString('hex'))
        .sort()
    t.deepEqual(shoulds, ['0100', '0101', '0102', '0103', '0104'])

    const musts = exp.must
        .map(te => (te as TermExp).term.toString('hex'))
        .sort()
    t.deepEqual(musts, ['02000000000019'])
})

test('numeric range exp level 1 ', t => {
    const exp = rangeToExp('f', BigInt(25), 1, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term.toString('hex'))
        .sort()
    t.deepEqual(shoulds, [
        '02000000001900',
        '02000000001901',
        '02000000001902',
        '02000000001903',
        '02000000001904'
    ])
})

test('numeric range exp level 2 ', t => {
    const exp = rangeToExp('f', BigInt(25), 2, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term.toString('hex'))
        .sort()
    t.deepEqual(shoulds, [
        '030000001900',
        '030000001901',
        '030000001902',
        '030000001903',
        '030000001904'
    ])
})

test('numeric range exp level 6 ', t => {
    const exp = rangeToExp('f', BigInt(25), 6, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term.toString('hex'))
        .sort()
    t.deepEqual(shoulds, ['0700', '0701', '0702', '0703', '0704'])
})

test('addRange 1 bucket at level 0', t => {
    const exp = addRange('f', BigInt(25), BigInt(26), 0)

    t.is(exp.toString(), '((f:0119) +f:02000000000000 )')

    const exp2 = addRange('f', BigInt(25), BigInt(29), 0)

    t.is(
        exp2.toString(),
        '((f:0119) (f:011a) (f:011b) (f:011c) +f:02000000000000 )'
    )
})

test('addRange 2 buckets at level 0', t => {
    const exp = addRange('f', BigInt(255), BigInt(257), 0)

    t.is(
        exp.toString(),
        '((f:01ff) +f:02000000000000 ),((f:0100) +f:02000000000001 )'
    )
})

test('addRange full overlap 1 bucket at level 0', t => {
    const exp = addRange('f', BigInt(255), BigInt(257 + 256), 0)

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:01ff) +f:02000000000000 )',
        '((f:02000000000001)  )',
        '((f:0100) +f:02000000000002 )'
    ]

    t.deepEqual(actual, expected)
})

test('addRange full overlap 1 bucket at level 1', t => {
    const exp = addRange('f', BigInt(255), BigInt(257 + 256 * 255), 0)

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:01ff) +f:02000000000000 )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:020000000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '((f:0100) +f:02000000000100 )'
    ]

    t.deepEqual(actual, expected)
})

test('addRange full overlap 1 bucket+1 at level 1', t => {
    const exp = addRange('f', BigInt(255), BigInt(257 + 256 * 256), 0)

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:01ff) +f:02000000000000 )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:020000000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '((f:02000000000100)  )',
        '((f:0100) +f:02000000000101 )'
    ]

    t.deepEqual(actual, expected)
})

test('addRange full overlap 1 bucket at level 6', t => {
    const exp = addRange('f', BigInt(255), BigInt(257 + 256 ** 6), 0)

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:01ff) +f:02000000000000 )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:020000000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:0300000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:04000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:050000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:0600${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '((f:02010000000000)  )',
        '((f:0100) +f:02010000000001 )'
    ]

    t.deepEqual(actual, expected)
})

test('addRange full overlap 1 bucket at level 6 + 2 at level 1', t => {
    const exp = addRange('f', BigInt(255), BigInt(257 + 256 ** 6 + 2 * 256), 0)

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:01ff) +f:02000000000000 )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:020000000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:0300000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:04000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:050000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:0600${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '((f:02010000000000) (f:02010000000001) (f:02010000000002)  )',
        '((f:0100) +f:02010000000003 )'
    ]

    t.deepEqual(actual, expected)
})

test('addRange full overlap 1 bucket at level 6 + 2 at level 3', t => {
    const exp = addRange(
        'f',
        BigInt(255),
        BigInt(257 + 256 ** 6 + 2 * 256 ** 3),
        0
    )

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:01ff) +f:02000000000000 )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:020000000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:0300000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:04000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:050000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:0600${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '((f:0401000000) (f:0401000001)  )',
        '((f:02010000020000)  )',
        '((f:0100) +f:02010000020001 )'
    ]

    t.deepEqual(actual, expected)
})

test('addRange full overlap 3 bucket at level 6 + 2 at level 3', t => {
    const exp = addRange(
        'f',
        BigInt(255),
        BigInt(257 + 3 * 256 ** 6 + 2 * 256 ** 3),
        0
    )

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:01ff) +f:02000000000000 )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:020000000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:0300000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:04000000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:050000${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '(' +
            toArray(range(1, 255))
                .map(i => `(f:0600${i.toString(16).padStart(2, '0')})`)
                .join(' ') +
            '  )',
        '((f:0701) (f:0702)  )',
        '((f:0403000000) (f:0403000001)  )',
        '((f:02030000020000)  )',
        '((f:0100) +f:02030000020001 )'
    ]

    t.deepEqual(actual, expected)
})
