import { test } from 'node:test'
import { W32023Upload, W32023UploadsFromNdjson } from './w32023.js'
import fromW32023ToW3up from './w32023-to-w3up.js'
import assert from 'assert'
import * as Link from 'multiformats/link'
import * as CAR from "@ucanto/transport/car"
import * as Client from '@ucanto/client'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as Server from "@ucanto/server"
import { ReadableStream } from 'stream/web'
import { migrate, migrateWithConcurrency } from './migrate-w32023-to-w3up.js'
import { IncomingMessage, createServer } from 'http'

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

await test(`can migrate with mock servers and concurrency`, async () => {
  const uploads = createEndlessUploads()
  /**
   * @type {Map<string, Array<{
   *   resolve:(v: any) => void
   *   reject: (err?: Error) => void
   * }>>}
   */
  class HangingRequests {
    constructor() {
      /**
       * @type {Map<string, Array<{
       *   resolve:(v: any) => void
       *   reject: (err?: Error) => void
       * }>>}
       */
      this.cidToHangs = new Map
    }
    get size() {
      let count = 0;
      for (const [, value] of this.cidToHangs.entries()) {
        count += value.length
      }
      return count
    }
    *[Symbol.iterator]() {
      for (const hangs of this.cidToHangs.values()) {
        yield * hangs
      }
    }
    delete(cid) {
      this.cidToHangs.delete(cid)
    }
    push(cid, { resolve, reject }) {
      const cleanup = () => {
        this.delete(cid)
      }
      const hangs = this.cidToHangs.get(cid) ?? []
      const hang = {
        resolve(v) {
          cleanup()
          resolve(v)
        },
        reject(e) {
          cleanup()
          reject(e)
        },
      }
      this.cidToHangs.set(cid, [...hangs, hang])
    }
  }
  const hangs = new HangingRequests
  const carFinder = createServer(createCarFinder({
    cid(req) {
      const lastPathSegmentMatch = req.url.match(/\/([^/]+)$/)
      const cid = lastPathSegmentMatch && lastPathSegmentMatch[1]
      return cid
    },
    headers(req) {
      return {
        'content-length': String(100),
      }
    },
    // hang all requests so we can control them.
    // and ensure concurrency
    async waitToRespond(req) {
      const cid = this.cid(req)
      await new Promise((resolve, reject) => {
        hangs.push(cid, { resolve, reject })
      })
    }
  }))
  carFinder.listen(0)

  try {
    const { url } = await listening(carFinder)
    const concurrency = 3

    const space = await ed25519.generate()
    const issuer = space
    const connection = Client.connect({
      id: issuer,
      codec: CAR.outbound,
      channel: await createMockW3upServer(),
    })    
    
    const aborter = new AbortController

    const migration = migrateWithConcurrency({
      source: uploads.readable,
      destination: new URL(space.did()),
      issuer,
      concurrency,
      signal: aborter.signal,
      w3up: connection,
      fetchPart(cid, { signal }) {
        return fetch(new URL(`/ipfs/${cid}`, url), { signal })
      }
    })
    const migrationEvents = []

    let maxHangSize = 0
    /**
     * @param {any} v - promise will resolve with this
     * @param {number} ms - milliseconds to defer
     */
    const defer = (v, ms=1000) => new Promise((resolve) => setTimeout(() => resolve(v)))

    /*
    In Parallel, do:
    * keep running the migration - knowing that for each cart part,
      the migration will call `fetchPart(cid)` which will request carFinder
      and not respond until we want it to. The hanging requests should
      occupy one concurrency 'slot' but as long as there is more concurrency,
      the migration should start fetching more car parts.
    * Monitor to make sure there are never more hanging connections at carFinder
      than the concurrency value, otherwise throw.
      End when the number of hanging connection equals concurrency.

    If the race completes, expect:
    * there are probably hanging connections at carFinder (assert on this)
    * ~ concurrency items were pulled from upstream uploads 
    */
    let migrateError
    await Promise.race([
      async function () {
        try {
          for await (const event of migration) {
            migrationEvents.push(event)
            if (aborter.signal.aborted) break
          }
        } catch (error) {
          migrateError = error
          aborter.abort(error)
        }
      }(),
      async function () {
        while (await defer(true)) {
          maxHangSize = Math.max(maxHangSize, hangs.size)
          if (hangs.size > concurrency) {
            throw new Error(`migration made a number of pending requests to fetchPart that exceeded options.concurrency, which shouldnt happen`)
          }
          if (aborter.signal.aborted) return
          if (hangs.size === concurrency) {
            await new Promise((resolve) => setImmediate(resolve))
            break;
          }
        }
      }(),
    ])

    assert.equal(hangs.size, concurrency, `hangs.size === concurrency`)

    // we dont promise it will be exactly concurrency + 2, but there shouldn't be any reason to fetch more than that
    assert.ok(uploads.pulledCount <= (concurrency+2), 'didnt pull more than needed to satisfy concurrency')

    for (const { resolve } of hangs) resolve()
    assert.equal(hangs.size, 0)

    // tick to allow migration to flow
    await new Promise((resolve) => setImmediate(resolve))

    // stop the migration
    aborter.abort()

    assert.ok( ! migrateError, 'migrate asyncgenerator didnt throw')
    assert.equal(migrationEvents.length, concurrency)
    assert.equal(maxHangSize, concurrency, `maxHangSize === concurrency`)
  } finally {
    carFinder.close()
    for (const { reject } of hangs) {
      reject(new Error('hang still running when test ended'))
    }
  }
})

/**
 * @param {object} options - options
 * @param {(request: IncomingMessage) => string|undefined} options.cid - given request, return a relevant CAR CID to find
 * @param {(request: IncomingMessage) => Record<string,string>} options.headers - given a request, return map that should be used for http response headers, e.g. to add a content-length header like w3s.link does.
 * @param {(request: IncomingMessage) => Promise<any>} [options.waitToRespond] - if provided, this can return a promise that can defer responding
 * @returns {import('http').RequestListener} request listener that mocks w3s.link
 */
function createCarFinder(options) {
  return (req, res) => {
    (options.waitToRespond?.(req) ?? Promise.resolve()).then(() => {
      const cid = options.cid(req)
      if ( ! cid) {
        res.writeHead(404)
        res.end('cant determine cid');
        return;
      }
      const headers = options.headers(req) ?? {}
      res.writeHead(200, {
        ...headers
      })
      res.end()
    })
  }
}

/**
 * create an infinite stream of uploads
 */
function createEndlessUploads() {
  let pulledCount = 0
  const readable = new globalThis.ReadableStream({
    async pull(controller) {
      const upload = W32023Upload.from(uploadsNdjson.split('\n').filter(Boolean)[0])
      pulledCount++
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
 * @param {object} [options] - options
 * @param {Promise<import('@ucanto/interface').Signer>} [options.id] - 
 */
async function createMockW3upServer(options={}) {
  const id = await (options.id || ed25519.generate())
  const invocations = []
  const server = Server.create({
    id,
    service: {
      store: {
        add(invocation, ctx) {
          invocations.push(invocation)
          return {
            ok: {
              status: 'done',
            }
          }
        }
      },
      upload: {
        add(invocation, ctx) {
          invocations.push(invocation)
          return {
            ok: {}
          }
        }
      }
    },
    codec: CAR.inbound,
    validateAuthorization: () => ({ ok: {} }),
  })
  return Object.assign(Object.create(server), {
    get invocations() { return invocations }
  })
}
