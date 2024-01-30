import { test } from 'node:test'
import { W32023Upload, W32023UploadsFromNdjson } from './w32023.js'
import fromW32023ToW3up from './w32023-to-w3up.js'
import assert from 'assert'
import * as Link from 'multiformats/link'
import * as CAR from "@ucanto/transport/car"
import * as Client from '@ucanto/client'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as Server from "@ucanto/server"
import { ReadableStream, TransformStream } from 'stream/web'
import { migrate } from './migrate-w32023-to-w3up.js'
import { Parallel } from 'parallel-transform-web'
import { createServer } from 'http'

/** example uploads from `w3 list --json` */
const uploadsNdjson = `\
{"_id":"1","type":"Car","name":"Upload at 2024-01-19T04:40:04.490Z","created":"2024-01-19T04:40:04.49+00:00","updated":"2024-01-19T04:40:04.49+00:00","cid":"bafybeihtddvvufnzdcetubq5mbv2rvgjchlipf6y7esei5qzg4r7re7rju","dagSize":2949303,"pins":[{"status":"Pinned","updated":"2024-01-19T04:40:04.49+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaieraclriozt34fk5ej3aa7k67es2hyq5zyc3ohivgbee4qeyyeroqb4a"],"deals":[]}\
`

// add later 

await test('can convert one upload to a store/add', async () => {
  const upload = W32023Upload.from(uploadsNdjson.split('\n').filter(Boolean)[0])
  const adds = []
  for await (const a of fromW32023ToW3up.toStoreAdd(upload)) { adds.push(a) }
  assert.equal(adds.length, 1)
  assert.equal(adds[0].nb.size, 2949554)
  assert.equal(adds[0].nb.link.toString(), Link.parse("bagbaieraclriozt34fk5ej3aa7k67es2hyq5zyc3ohivgbee4qeyyeroqb4a").toString())
})

await test('can convert stream of json to stream of uploads', async () => {
  const ndjson = new ReadableStream({
    start(c) {
      c.enqueue(new TextEncoder().encode(uploadsNdjson))
      c.close()
    }
  })
  const uploads = new W32023UploadsFromNdjson(ndjson)
  let uploadCount = 0;
  for await (const upload of uploads) {
    uploadCount++
    assert.ok(typeof upload.cid, 'string')
  }
  assert.equal(uploadCount, 1)
})

await test('can run migration to mock server', async () => {
  let storeAddInvocations = []
  let uploadAddInvocations = []
  const server = Server.create({
    id: await ed25519.generate(),
    service: {
      store: {
        add(invocation, ctx) {
          storeAddInvocations.push(invocation)
          return {
            ok: {
              status: 'done',
            }
          }
        }
      },
      upload: {
        add(invocation, ctx) {
          uploadAddInvocations.push(invocation)
          return {
            ok: {}
          }
        }
      }
    },
    codec: CAR.inbound,
    validateAuthorization: () => ({ ok: {} }),
  })

  const space = await ed25519.generate()
  const issuer = space
  const connection = Client.connect({
    id: issuer,
    codec: CAR.outbound,
    channel: server,
  })
  const uploads = new W32023UploadsFromNdjson(new ReadableStream({
    start(c) {
      c.enqueue(new TextEncoder().encode(uploadsNdjson))
      c.close()
    }
  }))
  const aborter = new AbortController
  const migration = migrate({
    issuer,
    w3up: connection,
    authorization: [],
    source: uploads,
    destination: new URL(space.did()),
    signal: aborter.signal,
  })
  const events = []
  for await (const event of migration) {
    events.push(event)
  }
  assert.equal(events.length, 2)
  assert.ok(events.find(e => e.object.type.toLowerCase() === 'upload'))
  assert.ok(events.find(e => e.object.type.toLowerCase() === 'car'))
})

for (let concurrency = 1; concurrency <= 4; concurrency++) {
  await test(`can map to store/add invocations in parallel concurrency=${concurrency}`, async () => {
    let pulled = 0;
    const createUploads = () => new globalThis.ReadableStream({
      async pull(controller) {
        console.log('start pull from upstream uploads', { pulled })
        const upload = W32023Upload.from(uploadsNdjson.split('\n').filter(Boolean)[0])
        pulled++
        console.log('incremented pulled', pulled)
        controller.enqueue(upload)
      }
    })
    const delay = n => new Promise(resolve => setImmediate(() => resolve(n)))
    /** @typedef {TransformStream} */
    const parallelize = new Parallel(concurrency, delay)
    const transformed = createUploads().pipeThrough(parallelize)
    const pull1 = await take(transformed.getReader())
    assert.equal(pulled, concurrency + 1)
    console.log('pull1', pull1)
    console.log('pulled', pulled)
  })
}

/**
 * transformer that transforms each input to N outputs as yielded by a
 * fn (I) => AsyncIterable<O>
 * @template I
 * @template O
 * @implements {Transformer<I, O>}
 */
class SpreadTransformer {
  /**
   * @param {(input: I) => AsyncIterable<O>} spread - convert an input to zero or more outputs
   */
  constructor(spread) {
    this.spread = spread
  }
  async transform(input, controller) {
    for await (const output of this.spread(input)) {
      controller.enqueue(output)
    }
  }
}

await test(`can transform w32023upload using TransformAddResponseFromHttp`, { only: true }, async () => {
  const concurrency = 1
  const uploads = createEndlessUploads()
  /** @type {Map<string,number>} */
  const contentLengthForCarPart = new Map
  const carFinder = createServer((req, res) => {
    const lastPathSegmentMatch = req.url.match(/\/([^/]+)$/)
    const cid = lastPathSegmentMatch && lastPathSegmentMatch[1]
    if ( ! cid) {
      res.writeHead(404)
      res.end('cant determine cid');
      return;
    }
    const contentLength = Math.floor(Math.random() * 100)
    contentLengthForCarPart.set(cid, contentLength)
    res.writeHead(200, {
      "content-length": contentLength,
    })
    res.end()
  })
  carFinder.listen(0)
  try {
    const { url } = await listening(carFinder)
    const transformerUploadToUploadPartResponse = new SpreadTransformer(
      /**
       * @param {W32023Upload} upload - upload whose parts should be fetched
       */
      async function * (upload) {
        for (const part of upload.parts) {
          const partUrl = new URL(`/ipfs/${part}`, url)
          const response = await fetch(partUrl)
          yield {
            upload,
            part,
            response,
          }
        }
      })
    const transformStreamAddingResponse = new TransformStream(transformerUploadToUploadPartResponse)
    const upload1 = await take(uploads.readable.getReader())
    const writer1 = transformStreamAddingResponse.writable.getWriter()
    try {
      await writer1.ready
      writer1.write(upload1)
      // close endless uploads. we only want 1 here.
      writer1.close()
    } finally {
      writer1.releaseLock()
    }
    // wrote one upload into transform.
    // now try to read
    const reader1 = transformStreamAddingResponse.readable.getReader()
    const i1 = await reader1.read()
    assert.ok(!i1.done)
    assert.ok(i1.value)
    assert.deepEqual(i1.value.upload, upload1)
    assert.ok(i1.value.response instanceof Response)
    assert.equal(i1.value.response.headers.get('content-length'), contentLengthForCarPart.get(i1.value.part))
    assert.equal(typeof i1.value.part, 'string')
  } finally {
    carFinder.close()
  }
  assert.equal(uploads.pulledCount, concurrency + 1)
})

/** create an infinite stream of uploads */
function createEndlessUploads() {
  let pulledCount = 0
  const readable = new globalThis.ReadableStream({
    async pull(controller) {
      const upload = W32023Upload.from(uploadsNdjson.split('\n').filter(Boolean)[0])
      pulledCount++
      console.log('incremented pulledCount', pulledCount)
      controller.enqueue(upload)
    }
  })
  return { get pulledCount() { return pulledCount }, readable }
}

/**
 * @param {import('http').Server} server - server that should be listening on the returned url
 */
async function listening(server) {
  const address = server.address()
  if (typeof address === 'string') throw new Error(`unexpected address string`)
  const { port } = address
  const url = new URL(`http://localhost:${port}/`)
  return { url }
}

/**
 * take one item from a stream reader, then release the lock
 * @template T
 * @param {ReadableStreamDefaultReader<T>} reader - reader to read from
 */
async function take(reader) {
  try {
    const { done, value } = await reader.read()
    if (done) {
      throw new Error('EOF')
    }
    return value
  } finally {
    reader.releaseLock()
  }
}
