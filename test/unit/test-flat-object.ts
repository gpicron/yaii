import test from 'ava'
import { flattenObject } from '../../src/lib/utils'
import * as util from "util"

test('basic', t => {
    const doc1 = {
        key: '%hTvbdzaK1+I4KdOfnNshpn0rm4KkWhygzKFtQpLcniY=.sha256',
        value: {
            previous: '%wSx3HGR4rT6ND64dJHrbxquWlQhSDKcwxbq/2o2ZHuc=.sha256',
            sequence: 5181,
            author: '@p13zSAiOpguI9nsawkGijsnMfWmFd5rlUNpzekEE+vI=.ed25519',
            timestamp: 1511038658871,
            hash: 'sha256',
            content: {
                type: 'npm-packages',
                mentions: [
                    {
                        name: 'npm:ssb-chess-db:1.0.1:latest',
                        link:
                            '&1EyJODelS/crUUXn09l6LFSWSpMYpIHXKeSSjNKMvUA=.sha256',
                        size: 3228,
                        dependencies: {
                            'flumeview-reduce': '^1.3.8',
                            'pull-defer': '^0.2.2',
                            'pull-iterable': '^0.1.0',
                            'pull-stream': '^3.6.1'
                        }
                    },
                    {
                        name: 'npm:ssb-chess-db:1.0.5',
                        link: '&1EyJODelS/FSWSpMYpIHXKeSSjNKMvUA=.sha256',
                        size: 3228,
                        dependencies: {
                            'flumeview-reduce': 'ccc',
                            'pull-defer': 'ddd',
                            'pull-iterable': 'eee',
                            'pull-stream': 'ssss'
                        }
                    }
                ]
            },
            signature:
                'poL1qaxJmgpmhjbt7TPik78/chuu0h7g0zqVVHh79xWQmfZjKlGY0oT/9DO+HZMdemRbjNgACWApBfKmJbCWCg==.sig.ed25519'
        },
        timestamp: 1585556464532
    }

    t.deepEqual(flattenObject(doc1), {

        key: '%hTvbdzaK1+I4KdOfnNshpn0rm4KkWhygzKFtQpLcniY=.sha256',
        timestamp: 1585556464532,
        'value.author': '@p13zSAiOpguI9nsawkGijsnMfWmFd5rlUNpzekEE+vI=.ed25519',
        'value.content.mentions.dependencies.flumeview-reduce': [
            '^1.3.8',
            'ccc'
        ],
        'value.content.mentions.dependencies.pull-defer': ['^0.2.2', 'ddd'],
        'value.content.mentions.dependencies.pull-iterable': ['^0.1.0', 'eee'],
        'value.content.mentions.dependencies.pull-stream': ['^3.6.1', 'ssss'],
        'value.content.mentions.link': [
            '&1EyJODelS/crUUXn09l6LFSWSpMYpIHXKeSSjNKMvUA=.sha256',
            '&1EyJODelS/FSWSpMYpIHXKeSSjNKMvUA=.sha256'
        ],
        'value.content.mentions.name': [
            'npm:ssb-chess-db:1.0.1:latest',
            'npm:ssb-chess-db:1.0.5'
        ],
        'value.content.mentions.size': [3228, 3228],
        'value.content.type': 'npm-packages',
        'value.hash': 'sha256',
        'value.previous':
            '%wSx3HGR4rT6ND64dJHrbxquWlQhSDKcwxbq/2o2ZHuc=.sha256',
        'value.sequence': 5181,
        'value.signature':
            'poL1qaxJmgpmhjbt7TPik78/chuu0h7g0zqVVHh79xWQmfZjKlGY0oT/9DO+HZMdemRbjNgACWApBfKmJbCWCg==.sig.ed25519',
        'value.timestamp': 1511038658871
    })
})

test('array in array', t => {
    const doc1 = {
        key: '%gFCY8FYZDButk8jKU2Y7dlGFhLALYmEIhn4mk2Uw4xs=.sha256',
        value: {
            previous: '%gPeAVkKViyZjeuye+HpvjJR+DT8kvZeMYkyo/wq/Pog=.sha256',
            sequence: 15053,
            author: '@EMovhfIrFk4NihAKnRNhrfRaqIhBv1Wj8pTxJNgvCCY=.ed25519',
            timestamp: 1542839933481,
            hash: 'sha256',
            content: {
                type: 'post',
                root: '%2P2FB5ZmZdxdIegGU6v9bEbwLi76ZMLjnnLlwQOLqIw=.sha256',
                branch: [ '%2P2FB5ZmZdxdIegGU6v9bEbwLi76ZMLjnnLlwQOLqIw=.sha256' ],
                text: 'talk to @xj9 ! and see [walkaway utah](%HXi9yEXJj4RsSlq6AqvP7WIQI0BybsRXTEX6izCF/5E=.sha256)',
                mentions: [
                    {
                        link: '%HXi9yEXJj4RsSlq6AqvP7WIQI0BybsRXTEX6izCF/5E=.sha256',
                        name: 'walkaway utah'
                    },
                    { name: 'xj9', link: 'false' },
                    {
                        name: 'xj9',
                        link: '@GqsSW1pLJq5qUbJuDAtm7MwwOhpf+Ur6BfDH0kZKCJc=.ed25519'
                    },
                    [
                        'xj9',
                        '@GqsSW1pLJq5qUbJuDAtm7MwwOhpf+Ur6BfDH0kZKCJc=.ed25519'
                    ]
                ]
            },
            signature: 'lPgWnIHCaw6m+K7MJvkBZKTPmh7knDPiUXmSCxqFCKHiQz6hXHO/duvuB2GihX+LCCISTOu00hWao6Xd8jpZBg==.sig.ed25519'
        },
        timestamp: 1585561328882.001
    }
    //t.log(util.inspect(flattenObject(doc1), false, null, true))

    t.deepEqual(flattenObject(doc1), {
        key: '%gFCY8FYZDButk8jKU2Y7dlGFhLALYmEIhn4mk2Uw4xs=.sha256',
        'value.previous': '%gPeAVkKViyZjeuye+HpvjJR+DT8kvZeMYkyo/wq/Pog=.sha256',
        'value.sequence': 15053,
        'value.author': '@EMovhfIrFk4NihAKnRNhrfRaqIhBv1Wj8pTxJNgvCCY=.ed25519',
        'value.timestamp': 1542839933481,
        'value.hash': 'sha256',
        'value.content.type': 'post',
        'value.content.root': '%2P2FB5ZmZdxdIegGU6v9bEbwLi76ZMLjnnLlwQOLqIw=.sha256',
        'value.content.branch': [ '%2P2FB5ZmZdxdIegGU6v9bEbwLi76ZMLjnnLlwQOLqIw=.sha256' ],
        'value.content.text': 'talk to @xj9 ! and see [walkaway utah](%HXi9yEXJj4RsSlq6AqvP7WIQI0BybsRXTEX6izCF/5E=.sha256)',
        'value.content.mentions.link': [
            '%HXi9yEXJj4RsSlq6AqvP7WIQI0BybsRXTEX6izCF/5E=.sha256',
            'false',
            '@GqsSW1pLJq5qUbJuDAtm7MwwOhpf+Ur6BfDH0kZKCJc=.ed25519'
        ],
        'value.content.mentions.name': [ 'walkaway utah', 'xj9', 'xj9' ],
        'value.content.mentions.0': 'xj9',
        'value.content.mentions.1': '@GqsSW1pLJq5qUbJuDAtm7MwwOhpf+Ur6BfDH0kZKCJc=.ed25519',
        'value.signature': 'lPgWnIHCaw6m+K7MJvkBZKTPmh7knDPiUXmSCxqFCKHiQz6hXHO/duvuB2GihX+LCCISTOu00hWao6Xd8jpZBg==.sig.ed25519',
        timestamp: 1585561328882.001
    })
})
