import test from 'ava'
import {
    addRange, numberToTerms,
    rangeToExp
} from '../../src/lib/internal/query-ir/query-ir'
import { range, toArray } from 'ix/iterable'
import {TermExp} from "../../src/lib/internal/query-ir/term-exp"
import {BooleanExpression} from "../../src/lib/internal/query-ir/boolean-exp"
import * as Long from "long"

test('number to terms', t => {
    let terms = numberToTerms(Number.MAX_SAFE_INTEGER)

    t.deepEqual(terms, [
        '1z',
        '9z',
        '8zz',
        '7zzz',
        '6zzzz',
        '5zzzzz',
        '4zzzzzz',
        '3zzzzzzz',
        '2zzzzzzzz',
    ])

    terms = numberToTerms(Number.MIN_SAFE_INTEGER)

    t.deepEqual(terms, [
        '1/',
        '9+',
        '8++',
        '7+++',
        '6++++',
        '5+++++',
        '4++++++',
        '3+++++++',
        '2++++++++',
    ])

    terms = numberToTerms(0)

    t.deepEqual(terms, [
        '1+',
        '9U',
        '8U+',
        '7U++',
        '6U+++',
        '5U++++',
        '4U+++++',
        '3U++++++',
        '2U+++++++',
    ])

    terms = numberToTerms(1)

    t.deepEqual(terms, [
        '1/',
        '9U',
        '8U+',
        '7U++',
        '6U+++',
        '5U++++',
        '4U+++++',
        '3U++++++',
        '2U+++++++',
    ])



    terms = numberToTerms(1 << 6)

    t.deepEqual(terms, [
        '1+',
        '9U',
        '8U+',
        '7U++',
        '6U+++',
        '5U++++',
        '4U+++++',
        '3U++++++',
        '2U++++++/',
    ])

    terms = numberToTerms(1 << 12)

    t.deepEqual(terms, [
        '1+',
        '9U',
        '8U+',
        '7U++',
        '6U+++',
        '5U++++',
        '4U+++++',
        '3U+++++/',
        '2U+++++/+',
    ])

    terms = numberToTerms(1 << 18)

    t.deepEqual(terms, [
        '1+',
        '9U',
        '8U+',
        '7U++',
        '6U+++',
        '5U++++',
        '4U++++/',
        '3U++++/+',
        '2U++++/++',
    ])

    terms = numberToTerms(2**(4*6))

    t.deepEqual(terms, [
        '1+',
        '9U',
        '8U+',
        '7U++',
        '6U+++',
        '5U+++/',
        '4U+++/+',
        '3U+++/++',
        '2U+++/+++',
    ])

    terms = numberToTerms(2**(5*6))

    t.deepEqual(terms, [
        '1+',
        '9U',
        '8U+',
        '7U++',
        '6U++/',
        '5U++/+',
        '4U++/++',
        '3U++/+++',
        '2U++/++++',
    ])

    terms = numberToTerms(2**(6*6))

    t.deepEqual(terms, [
        '1+',
        '9U',
        '8U+',
        '7U+/',
        '6U+/+',
        '5U+/++',
        '4U+/+++',
        '3U+/++++',
        '2U+/+++++',
    ])

    terms = numberToTerms(2**(7*6))

    t.deepEqual(terms, [
        '1+',
        '9U',
        '8U/',
        '7U/+',
        '6U/++',
        '5U/+++',
        '4U/++++',
        '3U/+++++',
        '2U/++++++',
    ])
    terms = numberToTerms(-1)

    t.deepEqual(terms, [
        '1z',
        '9T',
        '8Tz',
        '7Tzz',
        '6Tzzz',
        '5Tzzzz',
        '4Tzzzzz',
        '3Tzzzzzz',
        '2Tzzzzzzz',
    ])


})


test('numeric range exp level 0', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 0, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, ['1+', '1/', '10', '11', '12'])

    const musts = exp.must
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(musts, ['2+++++++N'])
})

test('numeric range exp level 1 ', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 1, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, [
        '2++++++N+',
        '2++++++N/',
        '2++++++N0',
        '2++++++N1',
        '2++++++N2'
    ])
})

test('numeric range exp level 2 ', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 2, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, [
        '3+++++N+',
        '3+++++N/',
        '3+++++N0',
        '3+++++N1',
        '3+++++N2'
    ])
})

test('numeric range exp level 3 ', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 3, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, [
        '4++++N+',
        '4++++N/',
        '4++++N0',
        '4++++N1',
        '4++++N2'
    ])
})

test('numeric range exp level 4 ', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 4, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, [
        '5+++N+',
        '5+++N/',
        '5+++N0',
        '5+++N1',
        '5+++N2'
    ])
})

test('numeric range exp level 5 ', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 5, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, [
        '6++N+',
        '6++N/',
        '6++N0',
        '6++N1',
        '6++N2'
    ])
})

test('numeric range exp level 6 ', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 6, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, [
        '7+N+',
        '7+N/',
        '7+N0',
        '7+N1',
        '7+N2'
    ])
})

test('numeric range exp level 7 ', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 7, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, [
        '8N+',
        '8N/',
        '8N0',
        '8N1',
        '8N2'
    ])
})

test('numeric range exp level 8 ', t => {
    const exp = rangeToExp('f', Long.fromNumber(25), 8, 0, 5) as BooleanExpression

    const shoulds = exp.should
        .map(te => (te as TermExp).term)
        .sort()
    t.deepEqual(shoulds, ['9+', '9/', '90', '91', '92'])
})

test('addRange 1 bucket at level 0', t => {
    const exp = addRange('f', Long.fromNumber(25), Long.fromNumber(26), 0)

    t.is(exp.toString(), '((f:1N) +f:2++++++++ )')

    const exp2 = addRange('f', Long.fromNumber(25), Long.fromNumber(29), 0)

    t.is(
        exp2.toString(),
        '((f:1N) (f:1O) (f:1P) (f:1Q) +f:2++++++++ )'
    )
})

test('addRange 2 buckets at level 0', t => {
    const exp = addRange('f', Long.fromNumber(63), Long.fromNumber(65), 0)

    t.is(
        exp.toString(),
        '((f:1z) +f:2++++++++ ),((f:1+) +f:2+++++++/ )'
    )
})

test('addRange full overlap 1 bucket at level 0', t => {
    const exp = addRange('f', Long.fromNumber(63), Long.fromNumber(65 + 64), 0)

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:1z) +f:2++++++++ )',
        '((f:2+++++++/)  )',
        '((f:1+) +f:2+++++++0 )'
    ]

    t.deepEqual(actual, expected)
})

const ENCODING_DIGITS = "+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

test('addRange full overlap 1 bucket at level 1', t => {
    const exp = addRange('f', Long.fromNumber(63), Long.fromNumber(65 + 64 * 63), 0)

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:1z) +f:2++++++++ )',
        '(' +
            toArray(range(1, 63))
                .map(i => `(f:2+++++++${ENCODING_DIGITS[i]})`)
                .join(' ') +
            '  )',
        '((f:1+) +f:2++++++/+ )'
    ]

    t.deepEqual(actual, expected)
})

test('addRange full overlap 1 bucket+1 at level 1', t => {
    const exp = addRange('f', Long.fromNumber(63), Long.fromNumber(65 + 64 * 64), 0)

    const actual = exp.map(e => e.toString())
    const expected = [
        '((f:1z) +f:2++++++++ )',
        '(' +
            toArray(range(1, 63))
                .map(i => `(f:2+++++++${ENCODING_DIGITS[i]})`)
                .join(' ') +
            '  )',
        '((f:2++++++/+)  )',
        '((f:1+) +f:2++++++// )'
    ]

    t.deepEqual(actual, expected)
})
