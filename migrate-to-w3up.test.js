import { test } from 'node:test'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import assert from 'node:assert'
import { exampleUpload1 } from './w32023.js'
import * as ed25519 from '@ucanto/principal/ed25519'
import { ReadableStream } from 'node:stream/web'
import { pipeline } from 'node:stream/promises'
import { createCarFinder, locate } from './test-utils.js'
import { createServer } from 'node:http'
import * as Server from '@ucanto/server'
import { Store, Upload } from '@web3-storage/capabilities'
import * as consumers from 'stream/consumers'
import * as CAR from '@ucanto/transport/car'
import { delegate } from '@ucanto/core'
import { encodeDelegationAsCid } from './w3-env.js'

const migrateToW3upPath = fileURLToPath(new URL('./migrate-to-w3up.js', import.meta.url))

/**
 * create a RequestListener that can be a mock up.web3.storage
 */
async function createMockW3up() {
  const service = {
    store: {
      add: Server.provide(Store.add, async (invocation) => {
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

test('uploadsNdJson | migrate-to-w3up --space <space.did>', async () => {
  const defaultCarFinderCarSize = 100
  const carFinder = createServer(createCarFinder({
    headers(req) { return { 'content-length': String(defaultCarFinderCarSize) } }
  }))
  const w3up = createServer(await createMockW3up())
  /**
   * test with running mock servers
   * @param {URL} carFinderUrl - url to mock w3s.link gateway, where migration will find cars
   * @param {URL} w3upUrl - url to mock w3up ucanto http endpoint
   */
  async function testMigration(
    carFinderUrl,
    w3upUrl,
  ) {
    const uploadLimit = 1
    let uploadsRemaining = uploadLimit
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

    const spaceA = await ed25519.generate()
    const migrationSession = await ed25519.generate()

    const spaceAAuthorizesMigrationSession = await delegate({
      issuer: spaceA,
      audience: migrationSession,
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

    const migrationProcess = spawnMigration([
      '--space', spaceA.did(),
      '--ipfs', carFinderUrl.toString(), 
      '--w3up', w3upUrl.toString(), 
    ], {
      ...process.env,
      W3_PRINCIPAL: ed25519.format(migrationSession),
      W3_PROOF: (await encodeDelegationAsCid(spaceAAuthorizesMigrationSession)).toString(),
    })
    await pipeline(uploads, migrationProcess.stdin)
    const migrationProcessExit = migrationProcess.exitCode || new Promise((resolve) => migrationProcess.on('exit', () => resolve()))

    const migrationEvents = []
    migrationProcess.stdout.on('data', (chunk) => {
      const parsed = JSON.parse(chunk.toString())
      migrationEvents.push(parsed)
    })

    const stderrChunks = []
    migrationProcess.stderr.on('data', (chunk) => {
      console.warn(chunk.toString())
      stderrChunks.push(chunk)
    })

    await migrationProcessExit
    assert.equal(migrationProcess.exitCode, 0, 'migrationProcess.exitCode is 0 (no error)')

    assert.equal(migrationEvents.length, uploadLimit)
    const e0 = migrationEvents[0]
    assert.ok(e0)
    // each part in original upload appears in event.parts map
    for (const partCid of e0.upload.parts) {
      assert.ok(partCid in e0.parts, `event.parts contains key ${partCid}`)
      const partMigration = e0.parts[partCid]
      assert.equal(partMigration.part, partCid)
      assert.deepEqual(partMigration.add.receipt.ran.capabilities[0].nb.link, { '/': partCid })
      assert.deepEqual(partMigration.add.receipt.out, {
        ok: {
          link: { '/': partCid },
          with: spaceA.did(),
          status: 'done',
          allocated: defaultCarFinderCarSize,
        }
      })
    }
    // event should also have the upload add receipt
    assert.equal(e0.add.receipt.type, 'Receipt')
    assert.deepEqual(e0.add.receipt.out.ok.root, {'/': e0.upload.cid})
  }

  // servers should listen
  await Promise.all([
    new Promise((resolve, reject) => {
      carFinder.listen(0)
      carFinder.on('listening', () => resolve())
    }),
    new Promise((resolve, reject) => {
      w3up.listen(0)
      w3up.on('listening', () => resolve())
    }),
  ])
  // run tests with urls of running servers,
  // and ensure servers close when done/error
  try {
    await testMigration(
      locate(carFinder).url,
      locate(w3up).url,
    )
  } finally {
    carFinder.close()
    w3up.close()
  }
})

/**
 * goal: verify it is possible to migrate uploads and tolerating errors along the way.
 * if one upload errors, the migration should be able to keep going, but keep a record of the failure.
 * after the migration run (with errors logged), the log of uploads that couldn't be migrated is effectively a new migration source.
 */
// todo

/**
 * spawn a migrate-to-w3up cli process
 * @param {string[]} args - cli args
 * @param {Record<string,string>} env - env vars
 */
function spawnMigration(args, env=process.env) {
  const proc = spawn(migrateToW3upPath, args, {
    env,
  })
  proc.on('error', (error) => {
    console.error('migration process error event', error)
  })
  return proc
}
