import { test } from 'node:test'
import {
  createMockW3up,
  spawnMigration,
  createCarFinder,
  locate,
  createUploadsStream,
  setupSpaceMigrationScenario,
} from "./test-utils.js"
import { createServer } from 'node:http'
import * as ed25519 from '@ucanto/principal/ed25519'
import { encodeDelegationAsCid } from '../src/w3-env.js'
import { pipeline } from 'node:stream/promises'
import { join } from 'node:path'
import { text } from 'node:stream/consumers'
import assert from 'node:assert'
import { createReadStream } from 'node:fs'
import * as fs from "fs/promises"
import { tmpdir } from 'node:os'
import readNDJSONStream from 'ndjson-readablestream'
import { Readable } from 'node:stream'
import { readUploadsFromUploadMigrationFailuresNdjson } from '../src/w3up-migration.js'

/** make a temporary file path that can be used for test migration logfiles  */
async function getTmpLogFilePath() {
  const tmp = await fs.realpath(tmpdir())
  const path = join(tmp, `migrate-to-w3up-test-${Date.now()}`)
  return path
}

await test('running migrate-to-w3up cli with a log file logs to the file passed as --log', async t => {
  // we'll migrate three uploads
  const uploads = createUploadsStream({ limit: 4 })
  // set things up so the first store/add invocation errors,
  // but all subsequent store/add invocations dont error.
  // also the second upload/add invocation will error (and rest succeed).
  // For the migrated uploads, we then expect:
  // 1. will fail due to underlying store/add error of upload part
  // 2. will succeed (store/add succeeds after first invocation, and the upload/add will succeed since it should be the first invocation)
  // 3. will fail due to failure in upload/add invocation
  // 4. will succeed (as would subsequent ones)
  let storeAddRequestCount = 0
  let uploadAddRequestCount = 0
  const w3upListener = createMockW3up({
    async onHandleStoreAdd(invocation) {
      try {
        if (0===storeAddRequestCount) {
          throw new Error('mocked store/add error')
        }
      } finally {
        storeAddRequestCount++
      }
    },
    async onHandleUploadAdd(invocation) {
      uploadAddRequestCount++
      if (uploadAddRequestCount == 2) {
        // error after first request
        throw new Error('mocked upload/add error')
      }
    }
  })
  const { carFinder, w3up, close } = await setupMockW3upServices(w3upListener)
  const { space, migrator, migratorCanAddToSpace } = await setupSpaceMigrationScenario()
  const tmpLogFilePath = await getTmpLogFilePath()
  // run test but dont worry about server cleanup
  const run = async () => {
    const migrationProcess = spawnMigration([
      '--space', space.did(),
      '--ipfs', carFinder.toString(),
      '--w3up', w3up.toString(),
      '--log', tmpLogFilePath,
    ], {
      ...process.env,
      W3_PRINCIPAL: ed25519.format(migrator),
      W3_PROOF: (await encodeDelegationAsCid(migratorCanAddToSpace)).toString(),
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

    const eventsFromLog = []
    for await (const event of readNDJSONStream(Readable.toWeb(createReadStream(tmpLogFilePath)))) {
      eventsFromLog.push(event)
    }

    assert.ok(eventsFromLog.length >= 1,
      'log file has at least one object in ndjson')
    assert.equal(
      eventsFromLog.filter(e => e.type === "UploadMigrationSuccess").length,
      2,
      'log has events of type UploadMigrationSuccess'
    )
    assert.equal(
      eventsFromLog.filter(e => e.type === "UploadMigrationFailure").length,
      2,
      'log has events of type UploadMigrationFailure'
    )

    assert.equal(stdoutText, "", 'there should be no stdout because we told it to write to a logfile instead')
    
    // stderr should be ndjson and have failures
    const stderrEvents = []
    for await (const e of readNDJSONStream(Readable.toWeb(Readable.from([new TextEncoder().encode(stderrText)])))) {
      stderrEvents.push(e)
      // stderrEvents should only be failures
      switch (e.type) {
        case "UploadMigrationFailure":
          // expected
          break;
        default:
          throw new Error(`unexpected stderr event type ${e.type}`)
      }
    }
    assert.equal(stderrEvents.length, 2)
    assert.equal(stderrEvents.filter(e => e.type === 'UploadMigrationFailure').length, 2)

    // let's see if we can use `migrate-to-w3up` log get-uploads-from-failures <log.path>
    const getUploadsFromFailuresProcess = spawnMigration(['log', 'get-uploads-from-failures', tmpLogFilePath])
    const uploadsFromFailures0 = []
    let getUploadsFromFailuresStderr
    await Promise.all([
      // wait for process exit
      new Promise((resolve) => getUploadsFromFailuresProcess.addListener('exit', () => resolve())),
      // read stdout as ndjson
      (async () => {
        for await (const e of readNDJSONStream(Readable.toWeb(getUploadsFromFailuresProcess.stdout))) {
          uploadsFromFailures0.push(e)
        }
      })(),
      (async () => {
        getUploadsFromFailuresStderr = await text(getUploadsFromFailuresProcess.stderr)
      })(),
    ])
    assert.equal(getUploadsFromFailuresProcess.exitCode, 0, 'exit code is 0 (no error)')
    assert.equal(uploadsFromFailures0.length, 2)
    for (const upload of uploadsFromFailures0) {
      assert.equal(typeof upload.cid, 'string')
      assert.ok(upload.parts?.length >= 1, 'upload has parts array with at least one part')
      for (const part of upload.parts) { assert.equal(typeof part, 'string') }
    }
    assert.ok(!getUploadsFromFailuresStderr, 'no stderr')

    // ok the first migration run ran as expected.
    // now let's verify we can use the logfile as a source of uploads (from UploadMigrationFailure events)
    // and do a second migration run
    const uploadsFromFailures = []
    for await (const u of readUploadsFromUploadMigrationFailuresNdjson(Readable.toWeb(createReadStream(tmpLogFilePath)))) {
      uploadsFromFailures.push(u)
    }
    assert.equal(uploadsFromFailures.length, 2)

    // actually run second migration process
    // with source equal to uploads extracted from UploadMigrationFailures in the previous log
    const tmpLogFilePath2 = await getTmpLogFilePath()
    const migrationProcess2 = spawnMigration([
      '--space', space.did(),
      '--ipfs', carFinder.toString(),
      '--w3up', w3up.toString(),
      '--log', tmpLogFilePath2,
    ], {
      ...process.env,
      W3_PRINCIPAL: ed25519.format(migrator),
      W3_PROOF: (await encodeDelegationAsCid(migratorCanAddToSpace)).toString(),
    })
    await pipeline(
      Readable.toWeb(spawnMigration(['log', 'get-uploads-from-failures', tmpLogFilePath]).stdout),
      migrationProcess2.stdin)
    const migrationProcess2Exit = migrationProcess2.exitCode || new Promise((resolve) => migrationProcess2.on('exit', () => resolve()))
    await migrationProcess2Exit
    assert.equal(migrationProcess2.exitCode, 0)

    // ok let's inspect that second log file and expect successful migration events
    // for the uploads that had failed to migrate before
    const eventsFromLog2 = []
    for await (const event of readNDJSONStream(Readable.toWeb(createReadStream(tmpLogFilePath2)))) {
      eventsFromLog2.push(event)
      switch (event.type) {
        case "UploadMigrationSuccess":
          // expected
          break;
        default:
          throw new Error(`unexpected migration event in log of type ${event.type}`, { cause: event })
      }
    }
    assert.equal(eventsFromLog2.length, 2)
  }
  try { await run() }
  finally { close(); }
})

await test('migrate-to-w3up logs to stderr if no --log passed', async t => {
  const uploads = createUploadsStream({ limit: 2 })
  let uploadAddRequestIndex = 0
  const w3upListener = createMockW3up({
    async onHandleUploadAdd() {
      try {
        if (uploadAddRequestIndex % 2 === 0) throw new Error('mock upload/add failure on even request indexes')
      } finally { uploadAddRequestIndex++ }
    }
  })
  const { carFinder, w3up, close } = await setupMockW3upServices(w3upListener)
  const { space, migrator, migratorCanAddToSpace } = await setupSpaceMigrationScenario()
  // run test but dont worry about server cleanup
  const run = async () => {
    const migrationProcess = spawnMigration([
      '--space', space.did(),
      '--ipfs', carFinder.toString(),
      '--w3up', w3up.toString(),
    ], {
      ...process.env,
      W3_PRINCIPAL: ed25519.format(migrator),
      W3_PROOF: (await encodeDelegationAsCid(migratorCanAddToSpace)).toString(),
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
    assert.ok(!stdoutText, 'no stdout')

    // stderr should have some ndjson events
    const stderrEvents = []
    for await (const event of readNDJSONStream(Readable.toWeb(Readable.from([new TextEncoder().encode(stderrText)])))) {
      stderrEvents.push(event)
    }
    assert.equal(stderrEvents.filter(e => e.type === 'UploadMigrationFailure').length, 1, 'stderr ndjson has UploadMigrationFailure event')

    const stderrLines = (stderrText || '').split('\n').filter(m => m.trim())
    const stderrLastLine = stderrLines[stderrLines.length - 1]
    assert.match(stderrLastLine, /failed to migrate 1\/2 uploads/)
  }
  try { await run() }
  finally { close(); }
})

/**
 * set up mock http servers that migration depends on: w3up and 'carFinder' e.g. w3s.link gateway
 * @param {Promise<import('node:http').RequestListener>} w3upListener - mock w3up http request listener
 */
async function setupMockW3upServices(w3upListener=createMockW3up()) {
  const defaultCarFinderCarSize = 100
  const carFinder = createServer(createCarFinder({
    headers(req) { return { 'content-length': String(defaultCarFinderCarSize) } }
  }))
  const w3up = createServer(await w3upListener)
  const servers = [carFinder, w3up]
  // wait for listening on available port
  await Promise.all(servers.map(async (s) => {
    await new Promise((resolve) => {
      w3up.on('listening', resolve)
      s.listen(0)
    })
  }))
  const carFinderUrl = (await locate(carFinder)).url
  const w3upUrl = (await locate(w3up)).url
  const close = () => {
    carFinder.close();
    w3up.close()
  }
  return { carFinder: carFinderUrl, w3up: w3upUrl, close }
}
