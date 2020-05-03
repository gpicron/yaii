import test from 'ava'
import { flattenObject } from '../../src/lib/utils'

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
