import { IncomingMessage } from 'http'
import { fileURLToPath } from 'node:url'
import { spawn } from 'node:child_process'
import * as Server from '@ucanto/server'
import { Store, Upload } from '@web3-storage/capabilities'
import * as consumers from 'stream/consumers'
import * as CAR from '@ucanto/transport/car'
import * as ed25519 from '@ucanto/principal/ed25519'
import { exampleUpload1 } from './w32023.js'
import { ReadableStream } from 'node:stream/web'
import { delegate } from '@ucanto/core'

/**
 * set up a simple set of objects for testing migration
 */
export async function setupSpaceMigrationScenario() {
  const spaceA = await ed25519.generate()
  const migrator = await ed25519.generate()
  const migratorCanAddToSpace = await delegate({
    issuer: spaceA,
    audience: migrator,
    capabilities: [
      {
        can: 'store/add',
        with: spaceA.did(),
      },
      {
        can: 'upload/add',
        with: spaceA.did(),
      }
    ]
  })
  return { space: spaceA, migrator, migratorCanAddToSpace }
}

/**
 * create a ReadableStream of uploads to migrate
 * @param {object} [options] - options
 * @param {number} [options.limit] - stop after this many uploads are read
 */
export function createUploadsStream({ limit = Infinity }={}) {
  let uploadsRemaining = limit
  const uploads = new ReadableStream({
    async pull(controller) {
      if (uploadsRemaining <= 0) {
        controller.close()
        return;
      }
      const text = JSON.stringify(exampleUpload1) + '\n'
      const bytes = new TextEncoder().encode(text)
      controller.enqueue(bytes)
      uploadsRemaining--
    }
  })  
  return uploads
}

/**
 * @param {object} options - options
 * @param {(request: IncomingMessage) => string|undefined} [options.cid] - given request, return a relevant CAR CID to find
 * @param {(request: IncomingMessage) => Record<string,string>} options.headers - given a request, return map that should be used for http response headers, e.g. to add a content-length header like w3s.link does.
 * @param {(request: IncomingMessage) => Promise<any>} [options.waitToRespond] - if provided, this can return a promise that can defer responding
 * @returns {import('http').RequestListener} request listener that mocks w3s.link
 */
export function createCarFinder(options) {
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
 * @param {import('http').Server} server - server that should be listening on the returned url
 */
export function locate(server) {
  const address = server.address()
  if (typeof address === 'string') throw new Error(`unexpected address string`)
  const { port } = address
  const url = new URL(`http://localhost:${port}/`)
  return { url }
}


export const migrateToW3upPath = fileURLToPath(new URL('./migrate-to-w3up.js', import.meta.url))

/**
 * create a RequestListener that can be a mock up.web3.storage
 * @param {object} [options] - options
 * @param {(invocation: import('@ucanto/server').ProviderInput<import('@ucanto/client').InferInvokedCapability<typeof Store.add>>) => Promise<void>} [options.onHandleStoreAdd] - called at start of store/add handler
 * @param {(invocation: import('@ucanto/server').ProviderInput<import('@ucanto/client').InferInvokedCapability<typeof Upload.add>>) => Promise<void>} [options.onHandleUploadAdd] - called at start of upload/add handler
 */
export async function createMockW3up(options={}) {
  const service = {
    store: {
      add: Server.provide(Store.add, async (invocation) => {
        await options.onHandleStoreAdd?.(invocation)
        /** @type {import('@web3-storage/access').StoreAddSuccessDone} */
        const success = {
          status: 'done',
          allocated: invocation.capability.nb.size,
          link: invocation.capability.nb.link,
          with: invocation.capability.with,
        }
        return {
          ok: success,
        }
      })
    },
    upload: {
      add: Server.provide(Upload.add, async (invocation) => {
        await options.onHandleUploadAdd?.(invocation)
        /** @type {import('@web3-storage/access').UploadAddSuccess} */
        const success = {
          root: invocation.capability.nb.root
        }
        return {
          ok: success,
        }
      })
    }
  }
  const serverId = (await ed25519.generate()).withDID('did:web:web3.storage')
  const server = Server.create({
    id: serverId,
    service,
    codec: CAR.inbound,
    validateAuthorization: () => ({ ok: {} }),
  })
  /** @type {import('node:http').RequestListener} */
  const listener = async (req, res) => {
    try {
      const requestBody = new Uint8Array(await consumers.arrayBuffer(req))
      const response = await server.request({
        body: requestBody,
        // @ts-expect-error slight mismatch. ignore like w3infra does
        headers: req.headers,
      })
      res.writeHead(200, response.headers)
      res.write(response.body)
    } catch (error) {
      console.error('error in mock w3up', error)
      res.writeHead(500)
      res.write(JSON.stringify(error))
    } finally {
      res.end()
    }
  }
  return listener
}

/**
 * spawn a migrate-to-w3up cli process
 * @param {string[]} args - cli args
 * @param {Record<string,string>} env - env vars
 */
export function spawnMigration(args, env=process.env) {
  const proc = spawn(migrateToW3upPath, args, {
    env,
  })
  proc.on('error', (error) => {
    console.error('migration process error event', error)
  })
  return proc
}
