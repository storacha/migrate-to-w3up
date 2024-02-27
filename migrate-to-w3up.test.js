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
import { text } from 'node:stream/consumers'

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

    let stdoutText
    let stderrText
    await Promise.all([
      migrationProcessExit,
      Promise.resolve().then(async () => {
        stdoutText = await text(migrationProcess.stdout) }),
      Promise.resolve().then(async () => {
        stderrText = await text(migrationProcess.stderr) }),
    ])
    assert.equal(migrationProcess.exitCode, 0, 'migrationProcess.exitCode is 0 (no error)')

    assert.equal(stdoutText, "", 'no stdout')
    assert.match(stderrText, /migrated 1 upload/, 'stderr reports on how many uploads were migrated')
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
