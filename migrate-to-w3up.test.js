import { test } from 'node:test'
import assert from 'node:assert'
import { exampleUpload1 } from './w32023.js'
import * as ed25519 from '@ucanto/principal/ed25519'
import { ReadableStream } from 'node:stream/web'
import { pipeline } from 'node:stream/promises'
import { createCarFinder, locate } from './test-utils.js'
import { createServer } from 'node:http'
import { delegate } from '@ucanto/core'
import { encodeDelegationAsCid } from './w3-env.js'
import { createMockW3up, spawnMigration } from "./test-utils.js"

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

    const stdoutChunks = []
    const migrationEvents = []
    migrationProcess.stdout.on('data', (chunk) => {
      // console.warn('stdout', chunk.toString())
      try { migrationEvents.push(JSON.parse(chunk.toString()))} catch (error) { /** pass */ }
      stdoutChunks.push(chunk)
    })

    const stderrChunks = []
    migrationProcess.stderr.on('data', (chunk) => {
      // console.warn('stderr', chunk.toString())
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
test('migrate-to-w3up logs errors', async () => {
  const defaultCarFinderCarSize = 100
  const carFinder = createServer(createCarFinder({
    headers(req) { return { 'content-length': String(defaultCarFinderCarSize) } }
  }))
  // mock w3up will error on first store/add but succeed after
  let didErrorOnFirstStoreAdd = false
  const w3up = createServer(await createMockW3up({
    async onHandleStoreAdd(invocation) {
      if (didErrorOnFirstStoreAdd) {
        return
      }
      didErrorOnFirstStoreAdd = true
      throw new Error('fake error on first store/add')
    }
  }))
  /**
   * test with running mock servers
   * @param {URL} carFinderUrl - url to mock w3s.link gateway, where migration will find cars
   * @param {URL} w3upUrl - url to mock w3up ucanto http endpoint
   */
  async function testMigration(
    carFinderUrl,
    w3upUrl,
  ) {
    const uploadLimit = 3
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
      // console.warn('stdout:', chunk.toString())
      const parsed = JSON.parse(chunk.toString())
      migrationEvents.push(parsed)
    })

    const stderrChunks = []
    migrationProcess.stderr.on('data', (chunk) => {
      // console.warn('stderr:', chunk.toString())
      // eslint-disable-next-line no-empty
      try { migrationEvents.push(JSON.parse(chunk.toString()))} catch {}
      stderrChunks.push(chunk)
    })

    await migrationProcessExit
    assert.equal(migrationProcess.exitCode, 1, 'migrationProcess.exitCode is 1 because not all uploads migrated ok')

    assert.equal(migrationEvents.length, uploadLimit)

    // first event should be a failure
    const [e0, ...eventsExcludingFirst] = migrationEvents
    assert.equal(e0.type, 'UploadMigrationFailure')
    for (const [, partMigration] of Object.entries(e0.parts)) {
      assert.ok('cause' in partMigration)
      assert.match(partMigration.cause.message, /fake error on first store\/add/i)
    }
    assert.equal(eventsExcludingFirst.length, uploadLimit-1) // -1 to exclude first.

    // remaining events should be success
    for (const event of eventsExcludingFirst) {
      assert.equal(event.type, 'UploadMigrationSuccess')
      assert.deepEqual(event.add.receipt.out.ok, {root:{'/': event.upload.cid}})
      for (const [partCid, partMigration] of Object.entries(event.parts)) {
        assert.deepEqual(partMigration.add.receipt.out.ok.link, {'/': partCid})
      }
    }
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

