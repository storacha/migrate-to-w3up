import { test } from 'node:test'
import {
  createMockW3up,
  spawnMigration,
  createCarFinder,
  locate,
  createUploadsStream,
  setupSpaceMigrationScenario,
} from "../test-utils.js"
import { createServer } from 'node:http'
import * as ed25519 from '@ucanto/principal/ed25519'
import { encodeDelegationAsCid } from '../w3-env.js'
import { pipeline } from 'node:stream/promises'
import { join } from 'node:path'
import { text } from 'node:stream/consumers'
import assert from 'node:assert'
import { createReadStream } from 'node:fs'
import * as fs from "fs/promises"
import { tmpdir } from 'node:os'
import readNDJSONStream from 'ndjson-readablestream'
import { Readable } from 'node:stream'

/** make a temporary file path that can be used for test migration logfiles  */
async function getTmpLogFilePath() {
  const tmp = await fs.mkdtemp(await fs.realpath(tmpdir()))
  const path = join(tmp, `migrate-to-w3up-test-${Date.now()}`)
  return path
}

await test('running migrate-to-w3up cli with a log file logs to the file passed as --log', async t => {
  const uploads = createUploadsStream({ limit: 1 })
  const { carFinder, w3up, close } = await setupMockW3upServices()
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
    assert.equal(migrationProcess.exitCode, 0,
      'migrationProcess.exitCode is 0 (no error)')
    // todo reenable
    // assert.equal(stdoutText, "", 'there should be no stdout because we told it to write to a logfile instead')
    console.log('stderrText', stderrText)
    console.log('stdoutText', stdoutText)
    console.log('tmpLogFilePath', tmpLogFilePath)
    const eventsFromLog = []
    for await (const event of readNDJSONStream(Readable.toWeb(createReadStream(tmpLogFilePath)))) {
      eventsFromLog.push(event)
    }
    console.log('eventsFromLog', eventsFromLog)
    assert.ok(eventsFromLog.length >= 1, 'log file has at least one object in ndjson')
    assert.ok(
      eventsFromLog.find(e => e.type === "UploadMigrationSuccess"),
      'log has at least one event of type UploadMigrationSuccess'
    )
  }
  try { await run() }
  finally { close(); }
})

/**
 * set up mock http servers that migration depends on: w3up and 'carFinder' e.g. w3s.link gateway
 */
async function setupMockW3upServices() {
  const defaultCarFinderCarSize = 100
  const carFinder = createServer(createCarFinder({
    headers(req) { return { 'content-length': String(defaultCarFinderCarSize) } }
  }))
  const w3up = createServer(await createMockW3up())
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
