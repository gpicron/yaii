import test from 'ava'
import { DocPackedArray } from '../../src/lib/doc-packed-array'
import { Doc } from '../../src/yaii-types'

test('basic depth 2', t => {
    const doc1 = {
        key: '%hTvbdzaK1+I4KdOfnNshpn0rm4KkWhygzKFtQpLcniY=.sha256',
        value: {
            previous: '%wSx3HGR4rT6ND64dJHrbxquWlQhSDKcwxbq/2o2ZHuc=.sha256',
            sequence: 5181
        },
        timestamp: 1585556464532
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    t.deepEqual(docPacker.get(0), doc1)
})

test('array of number', t => {
    const doc1 = {
        key: '%hTvbdzaK1+I4KdOfnNshpn0rm4KkWhygzKFtQpLcniY=.sha256',
        value: [1, 2, 3]
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    t.deepEqual(docPacker.get(0), doc1)
})

test('array of string', t => {
    const doc1 = {
        key: '%hTvbdzaK1+I4KdOfnNshpn0rm4KkWhygzKFtQpLcniY=.sha256',
        value: ['a', 'aaze', 'qseaze']
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    t.deepEqual(docPacker.get(0), doc1)
})

test('example', t => {
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
                    }
                ]
            },
            signature:
                'poL1qaxJmgpmhjbt7TPik78/chuu0h7g0zqVVHh79xWQmfZjKlGY0oT/9DO+HZMdemRbjNgACWApBfKmJbCWCg==.sig.ed25519'
        },
        timestamp: 1585556464532
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    t.deepEqual(docPacker.get(0), doc1)
})

test('basic array of string', t => {
    const doc1 = {
        id: '14',
        text: 'this is a demo',
        token_data: ['abc', 'bcd'],
        number_data: 25
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    t.deepEqual(docPacker.get(0), doc1)
})

test('basic array of docs', t => {
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
            //       token_data: ["abc", "bcd"],
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

    const docPacker = new DocPackedArray()

    //testEncode(docs[2], docPacker.store)
    //testDecode(0, docPacker.store)

    for (const doc of docs) {
        docPacker.add(doc as Doc)
    }

    t.is(docPacker.length, 4)
    t.deepEqual(docPacker.get(0), docs[0] as Doc)
    t.deepEqual(docPacker.get(1), docs[1] as Doc)
    t.deepEqual(docPacker.get(2), docs[2] as Doc)
    t.deepEqual(docPacker.get(3), docs[3] as Doc)
})

test('complex', t => {
    const doc1 = {
        key: '%wSx3HGR4rT6ND64dJHrbxquWlQhSDKcwxbq/2o2ZHuc=.sha256',
        value: {
            previous: '%UAOLEhCeG+Il/+o4EN7iBSgO4FrKRLPRue0nlUgHbA8=.sha256',
            sequence: 5180,
            author: '@p13zSAiOpguI9nsawkGijsnMfWmFd5rlUNpzekEE+vI=.ed25519',
            timestamp: 1511038595380,
            hash: 'sha256',
            content: {
                type: 'npm-packages',
                mentions: [
                    {
                        name: 'npm:flumeview-level:2.1.0:',
                        link:
                            '&kMkK0xIFddGivoGxohwfAoHe0YWIw+Of2QwI2rW1D3U=.sha256',
                        size: 11967,
                        dependencies: {
                            bytewise: '^1.1.0',
                            'explain-error': '^1.0.4',
                            level: '^1.7.0',
                            ltgt: '^2.1.3',
                            mkdirp: '^0.5.1',
                            obv: '0.0.0',
                            'pull-level': '^2.0.3',
                            'pull-paramap': '^1.2.1',
                            'pull-stream': '^3.5.0',
                            a: '^1.1.1',
                            b: '^2.0.3',
                            c: '^1.2.1',
                            d: '^1.2.1',
                            e: '^1.2.1',
                            f: '^1.2.1',
                            g: '^1.2.1',
                            h: '^1.2.1',
                            i: '^1.2.1',
                            j: '^1.2.1',
                            k: '^1.2.1'
                        }
                    },
                    {
                        name: 'npm:node-abi:2.1.2:',
                        link:
                            '&0G4wHVLwl+/PB+y93kAwNUK2tzVuclb5kM9qDb7gFus=.sha256',
                        size: 5743,
                        dependencies: { semver: '^5.4.1' }
                    },
                    {
                        name: 'npm:pull-iterable:0.1.0:',
                        link:
                            '&9X49JYkvWDr9O0ugwKFcM0YB1CftmIsLUT51F7d7Wi0=.sha256',
                        size: 2160
                    }
                ]
            },
            signature:
                'Ta8KZHmhPVXCxii4JQlwsLxz2KxrCtQiellY0nGHSvKD5Uv5FwamBGkJBxQYcCbVjuXD3c4Q36Ql1zSk4/VsBA==.sig.ed25519'
        },
        timestamp: 1585556464531
    }

    const docPacker = new DocPackedArray(10)

    //testEncode(doc1, docPacker.store)
    // @ts-ignore
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    // @ts-ignore
    t.deepEqual(docPacker.get(0), doc1)
})

test('complex 2', t => {
    const doc1 = {
        key: '%vV4Os2DzgWBUkcNw+zJAWryvYKjB0r6ZDY5B19fTOPo=.sha256',
        value: {
            previous: '%UPdJ8JDxmV8v3/QZ/AUAc6f0OAoe1p66s8l6qmRfBQ8=.sha256',
            sequence: 102,
            author: '@FJxH29WOcGbN0SuoasQo/mAXybo0lrMKbZDjpFlSrYs=.ed25519',
            timestamp: 1562291058436,
            hash: 'sha256',
            content: {
                type: 'npm-packages',
                mentions: [
                    {
                        name: 'npm:repetime-la-data:1.0.0:latest',
                        link:
                            '&Bl0x8jlykyBAtXjGrmlLhU/D5n1CqYq9ZOrhxi3Mkes=.sha256',
                        size: 2067,
                        shasum: '456c623d2a57483a7afb8feb5041f84dac70497d',
                        license: 'MIT',
                        dependencies: {
                            'ansi-colors': '^4.1.1',
                            clipboardy: '^2.1.0',
                            'dat-node': '^3.5.15',
                            diffy: '^2.1.0',
                            'menu-string': '^1.3.0',
                            'strip-ansi': '^5.2.0'
                        }
                    }
                ],
                dependencyBranch: [
                    '%Cv6gojwESzitGWDJYB3FG5ZiqtUhC+iKfgOM27+FSZU=.sha256',
                    '%UPdJ8JDxmV8v3/QZ/AUAc6f0OAoe1p66s8l6qmRfBQ8=.sha256',
                    '%/+hjosi/2d5OdH921EXB+vDrJhomkacoFggqkwOzoWg=.sha256',
                    '%H/hyv9Ru2tEjEc+n3zsIXaNMmD2ZbXZzZo3h32N8eDg=.sha256',
                    '%fua3wd8QlKfiP4jBELplwvmvwzkRpNZ6ewKh74GaVo0=.sha256',
                    '%neim8EZM+ZV77HXJpvd89ZTJHN0D17jrXvYxiU83u90=.sha256',
                    '%hzpeRVSV2PNwyhbed9PezG2Wmg5b7qXgWEzcSEN8wHk=.sha256'
                ],
                versionBranch: []
            },
            signature:
                'lWM93c6UiIxhXhO19h4H/v0S32zbyebTnyCxa/oKK+VnghBoIDv2e1/DTgFk6AX8ihswUYIXyTuyetqsYXLKDQ==.sig.ed25519'
        },
        timestamp: 1585561165734
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    // @ts-ignore
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    // @ts-ignore
    t.deepEqual(docPacker.get(0), doc1)
})

test('complex 3', t => {
    const doc1 = {
        key: '%lN4GaRuFQCXOcE03AmfO5DXki3FOmw6UB0AQrIXJoQQ=.sha256',
        value: {
            previous: '%OrAfYzpEX4Rd526mSJxj4qfdG8I18eh1M4qZ43jB4C0=.sha256',
            sequence: 895,
            author: '@iii/pg320nKa62v1ohHctlhrXYPmY5BzZ1dRjypd7Cg=.ed25519',
            timestamp: 1523298262612,
            hash: 'sha256',
            content: {
                type: 'npm-packages',
                mentions: [
                    {
                        name: 'npm:restore-cursor:1.0.1:latest',
                        link:
                            '&YGOHgvNYd3VXPuks2atcKlZsiWkc8tomEvJWNrHrNbA=.sha256',
                        size: 1834,
                        shasum: '4c0c336fc0f72363ec0630d2d3cd00d7a0919285',
                        dependencies: {
                            'exit-hook': '^1.0.0',
                            onetime: '^1.0.0'
                        },
                        bundledDependencies: false
                    }
                ],
                dependencyBranch: [
                    '%8L4ub4Mme0MfEfwzNzgSUkU+NAD9+ZczR73SF+MH1OQ=.sha256',
                    '%cyJGkEideOwbzn3rwB2FT0/dS6vbT7CCEyZktzvSyQc=.sha256',
                    '%ZLXUPHkBNGEn9aIHl5IT4SD8jr7YF2fBSDtxQG+kX24=.sha256',
                    '%VEq/U4CWwOVmGAiI+U7jBv3N5uMbb0LLapFViNe0LK4=.sha256'
                ],
                versionBranch: [
                    '%cyJGkEideOwbzn3rwB2FT0/dS6vbT7CCEyZktzvSyQc=.sha256',
                    '%eSt/DZrx0rsQ/T4kA+Tn0vyPWpzfDE91CWtZXmI0X2I=.sha256'
                ]
            },
            signature:
                'j0v3HC1C3tgJYTIoBGrOi+oT55Qo/OZW//DMbDd5Qz4ReUySwBOOTqCfKTyqOOClRHWNk6sZztZFmxwd1JRDCg==.sig.ed25519'
        },
        timestamp: 1585561192927.001
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    // @ts-ignore
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    // @ts-ignore
    t.deepEqual(docPacker.get(0), doc1)
})

test('complex 4', t => {
    const doc1 = {
        key: '%qk9ei9uUHg6gUyt1v4prxs1gpcp7O7k5dPz1XsFcQX8=.sha256',
        value: {
            previous: '%kbq6FNgusnwLAMczTvf5sM5D/2egxeSQj+17djlsfCo=.sha256',
            sequence: 2,
            author: '@9pxrnEE/3KXfo/iT3MDdMIfy7hvG+7hrHWAb/SByoXQ=.ed25519',
            timestamp: 1574786425940,
            hash: 'sha256',
            content: {
                type: 'post',
                text:
                    'Scuttlebutt sounds awesome, why have I ignored this until now?',
                mentions: []
            },
            signature:
                'edbvyXJxnbMXLOJ/nctPsRt1ISiFOTdESfyDCg4VfDBEzNFz8xkBVNB7tZEKqMAUz9CXcGV4JuW29vaID+pjCQ==.sig.ed25519'
        },
        timestamp: 1585556290062.001
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    // @ts-ignore
    docPacker.add(doc1)

    t.is(docPacker.length, 1)
    // @ts-ignore
    t.deepEqual(docPacker.get(0), doc1)
})

test('mixed array', t => {
    const doc1 = {
        mentions: [
            1,
            'string',
            { hello: 'world' },
            [3, 'hello', { world: 'hello' }]
        ]
    }

    const docPacker = new DocPackedArray()

    //testEncode(doc1, docPacker.store)
    // @ts-ignore
    docPacker.add(doc1)
    //testDecode(0, docPacker.store)
    t.is(docPacker.length, 1)
    // @ts-ignore
    t.deepEqual(docPacker.get(0), doc1)
})

test('array of record', t => {
    const doc1 = {
        key: '%hTvbdzaK1+I4KdOfnNshpn0rm4KkWhygzKFtQpLcniY=.sha256',
        value: [{ id: '1' }, { id: '2' }, { id: '3' }]
    }

    const docPacker = new DocPackedArray()

    docPacker.add(doc1)
    testDecode(0, docPacker.store)

    t.is(docPacker.length, 1)
    t.deepEqual(docPacker.get(0), doc1)
})

test('array of msgs', t => {
    const doc1 = [
        {
            key: '%2iqaCVWTsOjc6ucNLFAbPUtzVNXaeaNc2ZWATqRPF/4=.sha256',
            value: {
                previous:
                    '%M0usc5R7GqM1pHRSI9/xNWmCdfpoHN8GndIBeBs3zjc=.sha256',
                sequence: 553,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1584977031638,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@/M1m0RA6LNj4ECe2K0jBJbkfzm9eyWbQGUdwAkRj0Fo=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    'usMSaoq4CGflgVth61X1QbeRks5o7LJmoXzwU4B5GKXH/LGExMBO0x7NiU1gr/HErUhKlHIhRL5lOOgCjktGCg==.sig.ed25519'
            },
            timestamp: 1585556289562
        },
        {
            key: '%sW9xX6fc3gbn0x/ZdvEas7CKmDMYw/+LSvKgG/DWeLY=.sha256',
            value: {
                previous:
                    '%2iqaCVWTsOjc6ucNLFAbPUtzVNXaeaNc2ZWATqRPF/4=.sha256',
                sequence: 554,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585022127417,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@52yNPbs6hmCtQJnKDL7vNLXUHzisibXKUdDKwH4U1y4=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    '/EVlKT4WNNYjCqd8Y/gRi7+MbvwrC9Z2bZXOpeJywyaf1OMeyC5UjDqkDAN1aFHt0iDRjajzM9Knz3aZaKY4Dg==.sig.ed25519'
            },
            timestamp: 1585556289562.001
        },
        {
            key: '%VAkQTEiSyiq+47CU079wP8CMPot0VmOL1WuDCy3NhsA=.sha256',
            value: {
                previous:
                    '%sW9xX6fc3gbn0x/ZdvEas7CKmDMYw/+LSvKgG/DWeLY=.sha256',
                sequence: 555,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585065497820,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@vn+763pCMe0Vj7PZBRRe9mrW1845Yn3xa+rtLwML3tQ=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    'f4d6OdOpJZmlcIUnFCD2XwWpsaXg5EwwjeMH6uvQrSOMC3yh5W+pLyXL3wDLPo5kIAg7xd0XEVM4UBxj1Uk7Bw==.sig.ed25519'
            },
            timestamp: 1585556289563
        },
        {
            key: '%d8uI/n+ZOmhUW0k5xWBqW45LQByvgr3QXcS3kkAFZWs=.sha256',
            value: {
                previous:
                    '%VAkQTEiSyiq+47CU079wP8CMPot0VmOL1WuDCy3NhsA=.sha256',
                sequence: 556,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585166334084,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@RgzR2ITht0O/Tv6ree4hWl9vyD/ryb9sxR9w17whW3w=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    'ABUhWH+j1QpOU+okXkeovmNlCME/TtdFEuR+AT0rR/PHxWDqHm0/EIxE4ZN1yaQCLGKZTkfImkMniTjyOcthAw==.sig.ed25519'
            },
            timestamp: 1585556289564
        },
        {
            key: '%fluRPTascIx+vD6yLzDPrMxnOnr7YcHScmWDOxX6OcM=.sha256',
            value: {
                previous:
                    '%d8uI/n+ZOmhUW0k5xWBqW45LQByvgr3QXcS3kkAFZWs=.sha256',
                sequence: 557,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585192349229,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@WNe9CjP87IDKpy3H5ZLPauEUm2pDX09Jnms8TfMyap4=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    'UDBOoO30P2OQF5qcXOZyx+UDBm9DmRcx1EXmFwcVLlwh0VZKH0ci5jaancYWyS6K38zxAG3RfaJrytSDH3iEAA==.sig.ed25519'
            },
            timestamp: 1585556289565
        },
        {
            key: '%g2yY29UcPYg+J8Bmv6YaXxQ9Ad3xZCkggZvx6Vl3zAM=.sha256',
            value: {
                previous:
                    '%fluRPTascIx+vD6yLzDPrMxnOnr7YcHScmWDOxX6OcM=.sha256',
                sequence: 558,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585421927744,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@k2/ohLQIU/sgzN1hcqvo1Hdwq7dQ6Vrl5E+NS/12hOU=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    'ZR8dsd9XlCQ7U3i+Z3Cxqc9X0qRjMvlZt4GO+IveiG9x8PEFFoigdTstPW75/DYNouLLObt7uWNtdul2gGjOCw==.sig.ed25519'
            },
            timestamp: 1585556289566
        },
        {
            key: '%CqPJcdMzlt1kL0w6n7/FbHG/APjj1t2nHjLjW8zPiRc=.sha256',
            value: {
                previous:
                    '%g2yY29UcPYg+J8Bmv6YaXxQ9Ad3xZCkggZvx6Vl3zAM=.sha256',
                sequence: 559,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585427410829,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@EVab2Gy9SojbrRUHyWB2zUyY9dXc6bigG1SJB0zTog4=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    'O74IYvrDn7V4TOlsimapMrsiFo6AOv0vXgRY9d/NAynwzfwK2o34KynJJ6vBu2EQKtIloa6WFtspYqKKwWIUCA==.sig.ed25519'
            },
            timestamp: 1585556289567
        },
        {
            key: '%CO9tptonF+XmktxpBr5ipP5H8mcm8plXPnNwCNvGxKI=.sha256',
            value: {
                previous:
                    '%CqPJcdMzlt1kL0w6n7/FbHG/APjj1t2nHjLjW8zPiRc=.sha256',
                sequence: 560,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585459829390,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@WsyfhKPc79DsIn/N+f10Z53aUfl4mQw5xL1aZhp2HLg=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    '7ag9TEEriENk0T6P6ouLBmoQV24K1BGx8K9s9gZx4LzgfSeiEWriC/Hk1p9jRePagSchTJf6qfb8lhD8hpYYBg==.sig.ed25519'
            },
            timestamp: 1585556289568
        },
        {
            key: '%sz715hIOi5PPx1A6Cw8muVm2ZgTd08IOQh3VZLYSqrg=.sha256',
            value: {
                previous:
                    '%CO9tptonF+XmktxpBr5ipP5H8mcm8plXPnNwCNvGxKI=.sha256',
                sequence: 561,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585507770519,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@eqEfvSFeyoOGCkvL7dBb3YCIaQHsPcyi4IM6+7DJWmQ=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    'ly+V6uJITFeI5577RZmY/IBSGA90XiiykRKTbo4zs1znXJjKyZRSm4gnSIvYJhpCCCGnv4K0kinbkaXmzgZhCg==.sig.ed25519'
            },
            timestamp: 1585556289569
        },
        {
            key: '%HDT9LPxPaf+wFSqWs2kzHxiWK+Wkc+dGIbVPQi+ojhA=.sha256',
            value: {
                previous:
                    '%sz715hIOi5PPx1A6Cw8muVm2ZgTd08IOQh3VZLYSqrg=.sha256',
                sequence: 562,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585508242396,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@bFF/xemlo/t7PjGFtDjZ20RvsfM7b8anx1JKUlBnSb8=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    'RD9v6fdp1TRAE+WQe6njAisM46nv/ZMCWKuUmtPKiQADDoHGdap62Xaw2RuVmErnVWVe6ayKOtUBEhQr6hjSAQ==.sig.ed25519'
            },
            timestamp: 1585556289570
        },
        {
            key: '%tob1Fyzy7AL2YyqO7DMwaSPLPA0ashY2+lwjgavZU5M=.sha256',
            value: {
                previous:
                    '%HDT9LPxPaf+wFSqWs2kzHxiWK+Wkc+dGIbVPQi+ojhA=.sha256',
                sequence: 563,
                author: '@4TG/WLESyhThgTvmi5W3baX//tbF0HyskFprREqHbyc=.ed25519',
                timestamp: 1585556287462,
                hash: 'sha256',
                content: {
                    type: 'contact',
                    contact:
                        '@gBZQVjIukvbX8Bs22vdTfAHMiVfE9nR+NvXQYVaqeIg=.ed25519',
                    following: true,
                    pub: true
                },
                signature:
                    '1DETgbtaVpgluY4vj0CGWwP+dCXzeEMJtq/YAi1FCqwuvlms3Qn8TMznIqhuPJKjY3njEKCAhF9akeVuuU1YCQ==.sig.ed25519'
            },
            timestamp: 1585556289570.001
        }
    ]

    const tested = []
    tested.push(doc1[9])
    tested.push(doc1[10])
    tested.push(doc1[0])
    tested.push(doc1[1])
    tested.push(doc1[2])
    tested.push(doc1[3])
    tested.push(doc1[4])
    tested.push(doc1[5])
    tested.push(doc1[6])
    tested.push(doc1[7])
    tested.push(doc1[8])

    const docPacker = new DocPackedArray()

    tested.forEach(e => docPacker.add(e))

    //t.is(docPacker.length, 11)
    for (let i = 0; i < docPacker.length; i++) {
        try {
            t.deepEqual(docPacker.get(i), tested[i], 'failure comparing ' + i)
        } catch (e) {
            t.log('while loading ' + i)
            throw e
        }
    }
})

function testDecode(pointer: number, store: any) {}

function testEncode(doc: any, store: any) {
    for (const [field, data] of Object.entries(doc)) {
        switch (field) {
            case 'key': {
                store.writeVarint32(1)
                store.writeVString(data)
                break
            }
            case 'value': {
                store.writeVarint32(2)
                // @ts-ignore
                store.writeVarint32(data.length)
                // @ts-ignore
                for (const doc of data) {
                    if (doc === null) {
                        store.writeVarint32(-1)
                    } else {
                        for (const [field, data] of Object.entries(doc)) {
                            switch (field) {
                                case 'id': {
                                    store.writeVarint32(1)
                                    store.writeVString(data)
                                    break
                                }
                                default:
                                    throw new Error(
                                        'failure with field .value.' + field
                                    )
                            }
                        }
                        store.writeVarint32(0x00)
                    }
                }

                // @ts-ignore
                function encodeMixedArray(array) {
                    store.writeVarint32(array.length)
                    for (const el of array) {
                        if (el === null) {
                            store.writeVarint32(-1)
                        } else {
                            const typeofdoc = typeof el
                            if (typeofdoc === 'number') {
                                store.writeVarint32(1)
                                store.writeDouble(el)
                            } else if (typeofdoc === 'string') {
                                store.writeVarint32(3)
                                store.writeVString(el)
                            } else if (Array.isArray(el)) {
                                store.writeVarint32(8)
                                encodeMixedArray(el)
                            } else {
                                store.writeVarint32(5)
                                {
                                    const doc = el
                                    if (doc === null) {
                                        store.writeVarint32(-1)
                                    } else {
                                        for (const [
                                            field,
                                            data
                                        ] of Object.entries(doc)) {
                                            switch (field) {
                                                case 'id': {
                                                    store.writeVarint32(1)
                                                    store.writeVString(data)
                                                    break
                                                }
                                                default:
                                                    throw new Error(
                                                        'failure with field .value.' +
                                                            field
                                                    )
                                            }
                                        }
                                        store.writeVarint32(0x00)
                                    }
                                }
                            }
                        }
                    } /* end for */
                } /* end function */
                encodeMixedArray(data)
                break
            }
            default:
                throw new Error('failure with field .' + field)
        }
    }
    store.writeVarint32(0x00)
}
