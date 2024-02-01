import { test } from 'node:test'
import { W32023Upload, W32023UploadsFromNdjson } from './w32023.js'
import assert from 'assert'
import * as CAR from "@ucanto/transport/car"
import * as Client from '@ucanto/client'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as Server from "@ucanto/server"
import { migrate } from './w32023-to-w3up.js'
import { IncomingMessage, createServer } from 'http'
import { MapCidToPromiseResolvers } from './utils.js'
import { ReadableStream, TransformStream } from 'stream/web'

/** example uploads from `w3 list --json` */
const uploadsNdjson = `\
{"_id":"1","type":"Car","name":"Upload at 2024-01-19T04:40:04.490Z","created":"2024-01-19T04:40:04.49+00:00","updated":"2024-01-19T04:40:04.49+00:00","cid":"bafybeihtddvvufnzdcetubq5mbv2rvgjchlipf6y7esei5qzg4r7re7rju","dagSize":2949303,"pins":[{"status":"Pinned","updated":"2024-01-19T04:40:04.49+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaieraclriozt34fk5ej3aa7k67es2hyq5zyc3ohivgbee4qeyyeroqb4a"],"deals":[]}\
`

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

/**
 * return a transformer that limits input stream to a max length
 * @param {number} size - size of desired limited stream
 */
function limit(size) {
  let remaining = size
  return {
    /**
     * @param {any} _ - written item
     * @param {TransformStreamDefaultController<any>} controller - enqueue output here
     */
    async transform(_, controller) {
      controller.enqueue(_)
      remaining--
      if (remaining <= 0) {
        controller.terminate()
      }
    }
  }
}

/**
 * @template {Record<string, any>} S
 * create reasonable options for a migration
 * @param {object} [options] - options
 * @param {Promise<import('@ucanto/interface').Transport.Channel<S>>} [options.channel] - channel
 */
async function createDefaultMigrationOptions({
  channel = createMockW3upServer()
}={}) {
  const space = await ed25519.generate()
  const issuer = space
  const w3up = Client.connect({
    id: issuer,
    codec: CAR.outbound,
    channel: await channel,
  })
  const aborter = new AbortController
  const destination = new URL(space.did())
  return {
    issuer,
    w3up,
    abort: aborter.abort.bind(aborter),
    signal: aborter.signal,
    destination
  }
}

await test('migration sends car bytes when store/add result says to', async () => {
  const uploads = createEndlessUploads()
  const readUploadsLimit = 3
  let uploadPartsExpected = 0
  const uploadsLimit1 = uploads.readable
    .pipeThrough(new TransformStream(limit(readUploadsLimit)))
    .pipeThrough(new TransformStream({
      async transform(upload, controller) {
        uploadPartsExpected += upload.parts.length
        controller.enqueue(upload)
      }
    }))

  const carFinder = createServer(createCarFinder({
    headers(req) { return { 'content-length': String(100) } }
  }))

  let carReceiverRequestCount = 0
  const carReceiver = createServer((req, res) => {
    carReceiverRequestCount++
    res.writeHead(201)
    res.end()
  })

  /**
   * @param {URL} carFinderUrl - url to w3s.link mock
   * @param {URL} carReceiverUrl - url that store/add should point to for uploading car parts
   */
  async function testMigration (carFinderUrl, carReceiverUrl) {
    const migrationOptions = {
      ...await createDefaultMigrationOptions({
        channel: createMockW3upServer({
          store: {
            async add(invocation) {
              /** @type {import('@web3-storage/access').StoreAddSuccessUpload} */
              const ok = {
                status: 'upload',
                with: invocation.capabilities[0].with,
                allocated: 1,
                link: invocation.capabilities[0].nb.link,
                url: carReceiverUrl.toString(),
                headers: {},
              }
              return {ok}
            }
          }
        })
      })
    }
  
    const migration = migrate({
      ...migrationOptions,
      source: uploadsLimit1,
      async fetchPart(cid, { signal }) {
        return fetch(new URL(`/ipfs/${cid}`, carFinderUrl))
      }
    })

    // finish migration
    try {
      for await (const _ of migration) {
        for (const [, part] of _.parts) {
          assert.ok(part.copy.response, 'part was copied')
          assert.equal(part.copy.response.status, 201)
        }
      }
    } catch (error) {
      migrationOptions.abort()
      throw error
    }

    assert.equal(carReceiverRequestCount, uploadPartsExpected)
  }

  carFinder.listen(0)
  carReceiver.listen(0)
  try {
    const {url: carFinderUrl} = await locate(carFinder)
    const {url: carReceiverUrl} = await locate(carReceiver)
    await testMigration(carFinderUrl, carReceiverUrl)
  } finally {
    carFinder.close()
    carReceiver.close()
  }
})

await test('can migrate with mock servers and concurrency', async () => {
  const uploads = createEndlessUploads()
  const hangs = new MapCidToPromiseResolvers
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
    const { url } = await locate(carFinder)
    const concurrency = 3

    const space = await ed25519.generate()
    const issuer = space
    const connection = Client.connect({
      id: issuer,
      codec: CAR.outbound,
      channel: await createMockW3upServer(),
    })

    const aborter = new AbortController

    const migration = migrate({
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
    const defer = (v, ms = 1000) => new Promise((resolve) => setTimeout(() => resolve(v)))

    let migrateError
    await Promise.race([
      /** run migration and abort on errors */
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
      /**
       * Monitor to make sure there are never more hanging connections at carFinder
       * than the concurrency value, otherwise throw.
       * End when the number of hanging connection equals concurrency.
       */
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
    assert.ok(uploads.pulledCount <= (concurrency + 2), 'didnt pull more than needed to satisfy concurrency')

    for (const { resolve } of hangs) resolve()
    assert.equal(hangs.size, 0)

    // tick to allow migration to flow
    await new Promise((resolve) => setImmediate(resolve))

    // stop the migration
    aborter.abort()

    assert.ok(!migrateError, 'migrate asyncgenerator didnt throw')

    for (const event of migrationEvents) {
      assert.ok(event.upload instanceof W32023Upload, 'event.upload is a W32023Upload')
      assert.ok(event.parts instanceof Map, 'event.parts is a Map of part cid to migrated part')
      assert.equal(event.parts.size, event.upload.parts.length, 'migrated upload has one part for each part cid in input upload')
      assert.ok(event.add.receipt.out.ok)
      for (const [cid, migratedPart] of event.parts) {
        assert.deepEqual(migratedPart.upload, event.upload)
        assert.ok(migratedPart.upload.parts.includes(cid))
        assert.ok(migratedPart.add.receipt.out.ok)
      }
    }
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
 * @param {(request: IncomingMessage) => string|undefined} [options.cid] - given request, return a relevant CAR CID to find
 * @param {(request: IncomingMessage) => Record<string,string>} options.headers - given a request, return map that should be used for http response headers, e.g. to add a content-length header like w3s.link does.
 * @param {(request: IncomingMessage) => Promise<any>} [options.waitToRespond] - if provided, this can return a promise that can defer responding
 * @returns {import('http').RequestListener} request listener that mocks w3s.link
 */
function createCarFinder(options) {
  return (req, res) => {
    (options.waitToRespond?.(req) ?? Promise.resolve()).then(() => {
      const getCid = options.cid ?? function (req) {
        const lastPathSegmentMatch = req.url.match(/\/([^/]+)$/)
        const cid = lastPathSegmentMatch && lastPathSegmentMatch[1]
        return cid
      }
      const cid = getCid(req)
      if (!cid) {
        res.writeHead(404)
        res.end('cant determine cid');
        return;
      }
      const headers = options.headers(req) ?? {}
      if (!Object.keys(headers).find(h => h.match(/content-length/i))) {
        throw new Error('carFinder response headers must include content-length')
      }
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
  const readable = new ReadableStream({
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
async function locate(server) {
  const address = server.address()
  if (typeof address === 'string') throw new Error(`unexpected address string`)
  const { port } = address
  const url = new URL(`http://localhost:${port}/`)
  return { url }
}

/**
 * @param {object} [options] - options
 * @param {Promise<import('@ucanto/interface').Signer>} [options.id] - 
 * @param {object} [options.store] - store service
 * @param {(i: import('@ucanto/interface').IssuedInvocation<import('@web3-storage/access').StoreAdd>) => Promise<import('@ucanto/validator').Result<import('@web3-storage/access').StoreAddSuccess>>} [options.store.add] - store/add handler
 */
async function createMockW3upServer(options = {}) {
  const id = await (options.id || ed25519.generate())
  const invocations = []
  const server = Server.create({
    id,
    service: {
      store: {
        add(invocation, ctx) {
          invocations.push(invocation)
          if (options?.store?.add) {
            return options.store.add(invocation)
          }
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
