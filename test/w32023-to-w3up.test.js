import { test } from 'node:test'
import { W32023Upload, W32023UploadsFromNdjson } from '../src/w32023.js'
import assert from 'assert'
import * as CAR from "@ucanto/transport/car"
import * as Client from '@ucanto/client'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as Server from "@ucanto/server"
import { migrate } from '../src/w32023-to-w3up.js'
import { createServer } from 'http'
import { MapCidToPromiseResolvers } from '../src/utils.js'
import { ReadableStream, TransformStream } from 'stream/web'
import { UploadMigrationFailure, UploadPartMigrationFailure } from '../src/w3up-migration.js'
import { createCarFinder, locate } from './test-utils.js'

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
    const {url: carFinderUrl} = locate(carFinder)
    const {url: carReceiverUrl} = locate(carReceiver)
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
    const { url } = locate(carFinder)
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
      async fetchPart(cid, { signal }) {
        const response = await fetch(new URL(`/ipfs/${cid}`, url), { signal })
        return response
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
    await new Promise((resolve, reject) => carFinder.close(error => error ? reject(error) : resolve()))
    for (const { reject } of hangs) {
      reject(new Error('hang still running when test ended'))
    }
  }
})

await test('can migrate in a way that throws when encountering status=upload', async () => {
  let uploadLimit = 3
  let uploadLimitRemaining = uploadLimit
  const uploads = createEndlessUploads().readable.pipeThrough(new TransformStream({
    async transform(chunk, controller) {
      if (uploadLimitRemaining <= 0) throw new Error('limit reached');
      controller.enqueue(chunk)
      if (--uploadLimitRemaining <= 0) {
        controller.terminate()
      }
    }
  }))
  // mock w3s.link. It just needs to be realistic-ish so the migration can build a store/add invocation
  // to send to the mock w3up
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
  }))
  carFinder.listen(0)
  await new Promise((resolve) => carFinder.addListener('listening', () => resolve()))
  try {
    await testCanMigrateWithNoCopy()
  } finally {
    carFinder.close()
  }
  /**
   * test with a running carFinder service.
   */
  async function testCanMigrateWithNoCopy() {
    const { url } = locate(carFinder)
    const space = await ed25519.generate()
    const issuer = space
    const connection = Client.connect({
      id: issuer,
      codec: CAR.outbound,
      // mock w3up will always return status=upload.
      // we'll call migrate() in such a way that it throws when encountering this
      // and assert that the throw happens
      channel: await createMockW3upServer({
        store: {
          async add(invocation) {
            /** @type {import('@web3-storage/access').StoreAddSuccessUpload} */
            const response = {
              status: 'upload',
              url: 'https://example.com',
              headers: {},
              allocated: 0,
              with: invocation.capabilities[0].with,
              link: invocation.capabilities[0].nb.link
            }
            return {
              ok: response
            }
          }
        }
      }),
    })
    const aborter = new AbortController
    let onStoreAddReceiptCallCount = 0;
    const migration = migrate({
      source: uploads,
      destination: new URL(space.did()),
      issuer,
      signal: aborter.signal,
      w3up: connection,
      fetchPart(cid, { signal }) {
        return fetch(new URL(`/ipfs/${cid}`, url), { signal })
      },
      onStoreAddReceipt(receipt) {
        onStoreAddReceiptCallCount++
        if (receipt.out.ok.status !== 'done') {
          throw new Error('unexpected store/add receipt')
        }
      }
    })
    let migrationEventCount = 0
    let failures = []
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    for await (const event of migration) {
      migrationEventCount++
      if (event instanceof UploadMigrationFailure) {
        failures.push(event)
        break;
      }
    }
    assert.equal(onStoreAddReceiptCallCount, 1, 'onStoreAddReceipt called')
    assert.equal(migrationEventCount, 1, 'one migration event yielded from migration')
    assert.equal(failures.length, 1)
  }
})

/**
 * sometimes the migration will get an unexpected error when invoking store/add for a piece.
 * When this happens, the asyncIterable should return an object that represents the failure to migrate an upload.
 * Then, whoever is doing the iterable can decide how to respond: either by aborting the migration or by continuing onward.
 */
await test('can migrate tolerating store/add invocation errors', async () => {
  // we'll have 3 uploads to migrate.
  // all these uploads only have one part.
  // We'll mock out the server side such that the first upload migrates ok, the second errors, and the third is ok
  const uploads = createEndlessUploads({ limit: 3 }).readable
  // mock w3s.link. It just needs to be realistic-ish so the migration can build a store/add invocation
  // to send to the mock w3up
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
  }))
  carFinder.listen(0)
  await new Promise((resolve) => carFinder.addListener('listening', () => resolve()))
  try {
    await testCanMigrateToleratingErrors((locate(carFinder)).url)
  } finally {
    carFinder.close()
  }
  /**
   * test with a running carFinder service.
   * @param {URL} carFinderLocation - url to car finder service
   */
  async function testCanMigrateToleratingErrors(carFinderLocation) {
    const space = await ed25519.generate()
    const issuer = space
    let storeAddResponseCount = 0
    const connection = Client.connect({
      id: issuer,
      codec: CAR.outbound,
      // mock w3up so the first store/add invocation works, and all subsequent
      // store/add invocations result in an unexpected error (i.e. simulated unexpected 500)
      channel: await createMockW3upServer({
        store: {
          async add(invocation) {
            let error
            /** @type {import('@web3-storage/access').StoreAddSuccessDone} */
            let ok
            // error on second
            if (storeAddResponseCount === 1) {
              error = new Error(`fake error from mock store/add handler in testCanMigrateToleratingErrors`)
            } else {
              // always say status=done because that way we dont need to mock out an upload target
              ok = {
                status: 'done',
                allocated: 0,
                with: invocation.capabilities[0].with,
                link: invocation.capabilities[0].nb.link
              }
            }
            storeAddResponseCount++
            if (error) {
              throw error
            }
            return { ok }
          }
        }
      }),
    })
    const aborter = new AbortController
    const migration = migrate({
      source: uploads,
      destination: new URL(space.did()),
      issuer,
      signal: aborter.signal,
      w3up: connection,
      fetchPart(cid, { signal }) {
        return fetch(new URL(`/ipfs/${cid}`, carFinderLocation), { signal })
      },
    })
    let migrationEventCount = 0
    let failures = []
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    for await (const event of migration) {
      migrationEventCount++
      if (event instanceof UploadMigrationFailure) {
        failures.push(event)
      }
    }
    assert.equal(migrationEventCount, 3, 'migration events yielded from migration')
    assert.equal(failures.length, 1)
    const failure0 = failures[0]
    assert.ok(failure0.upload.cid)
    assert.match(failure0.cause.toString(), /failed to migrate \d+\/\d+ upload parts/i)
    assert.equal(failure0.parts.size, 1)
    const failure0Part0 = failure0.parts.get(failure0.upload.parts[0])
    assert.ok(failure0Part0 instanceof UploadPartMigrationFailure)
    // @ts-expect-error tolerate 'unknown'
    assert.match(failure0Part0.cause.cause.message, /fake error from mock store\/add handler in testCanMigrateToleratingErrors/i)
  }
})

/**
 * create an infinite stream of uploads
 * @param {object} [options] options
 * @param {number} [options.limit] max upload to emit before closing stream
 */
function createEndlessUploads({ limit = Infinity }={}) {
  let pulledCount = 0
  /** @type {ReadableStream<W32023Upload>} */
  const readable = new ReadableStream({
    async pull(controller) {
      if (pulledCount >= limit) {
        controller.close()
        return;
      }
      const upload = W32023Upload.from(uploadsNdjson.split('\n').filter(Boolean)[0])
      upload._id = Math.random().toString().slice(2)
      pulledCount++
      controller.enqueue(upload)
    }
  })
  return { get pulledCount() { return pulledCount }, readable }
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
            try {
              return options.store.add(invocation)
            } catch (error) {
              console.error('uncaught error calling createMockW3upServer options.store.add', error)
              return {
                error,
              }
            }
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
