#!/usr/bin/env node
/* eslint-disable @typescript-eslint/no-unused-vars */
import { W32023Upload, W32023UploadSummary, W32023UploadsFromNdjson } from "./w32023.js";
import { fileURLToPath } from 'node:url'
import fs, { createReadStream, createWriteStream } from 'node:fs'
import { Readable } from 'node:stream'
import * as w3up from "@web3-storage/w3up-client"
import { parseArgs } from 'node:util'
import { DID } from "@ucanto/validator"
import { StoreConf } from '@web3-storage/access/stores/store-conf'
import { select } from '@inquirer/prompts';
import confirm from '@inquirer/confirm';
import { Web3Storage } from 'web3.storage'
import promptForPassword from '@inquirer/password';
import { carPartToStoreAddNb, migrate } from "./w32023-to-w3up.js";
import { UploadMigrationFailure, UploadMigrationSuccess, UploadPartMigrationFailure, receiptToJson } from "./w3up-migration.js";
import { Store } from "@web3-storage/capabilities";
import { connect } from '@ucanto/client'
import { CAR, HTTP } from '@ucanto/transport'
import { StoreMemory } from "@web3-storage/access/stores/store-memory";
import * as ed25519Principal from '@ucanto/principal/ed25519'
import { parseW3Proof } from "./w3-env.js";
import inquirer from 'inquirer';
import readNDJSONStream from 'ndjson-readablestream'
import { stringToCarCid } from "./utils.js";

// if this file is being executed directly, run main() function
const isMain = (url, argv = process.argv) => fileURLToPath(url) === fs.realpathSync(argv[1])
if (isMain(import.meta.url, process.argv)) {
  await main(process.argv)
}

/**
 * get w3up-client with store from a good default store
 * @param {URL} w3upUrl - url to w3up ucanto endpoint
 */
async function getDefaultW3up(w3upUrl) {
  let store
  if (process.env.W3_PRINCIPAL) {
    store = new StoreMemory
  } else {
    // no W3_PRINCIPAL. Use store from w3cli that will have one
    store = new StoreConf({ profile: process.env.W3_STORE_NAME ?? 'w3cli' })
  }
  const principal = process.env.W3_PRINCIPAL
    ? ed25519Principal.parse(process.env.W3_PRINCIPAL)
    : undefined

  const connection = connect({
    id: { did: () => 'did:web:web3.storage' },
    codec: CAR.outbound,
    channel: HTTP.open({
      url: w3upUrl,
      method: 'POST',
    }),
  })
  const w3 = await w3up.create({
    principal,
    store,
    serviceConf: {
      upload: connection,
      access: connection,
      filecoin: connection,
    },
    receiptsEndpoint: new URL('/receipt/', w3upUrl),
  })
  if (process.env.W3_PROOF) {
    await w3.addSpace(await parseW3Proof(process.env.W3_PROOF))
  }
  return w3
}

/**
 * get a @web3-storage/access/agent instance with default store
 * @param {URL} w3upUrl - url to w3up ucanto endpoint
 */
async function getDefaultW3upAgent(w3upUrl) {
  const w3 = await getDefaultW3up(w3upUrl)
  // @ts-expect-error _agent is protected property
  const access = w3._agent
  return access
}

/**
 * main function that runs when this file is executed.
 * It reads configuration from cli args, env vars, etc,
 * then runs a migration using the `migration` function defined above.
 * @param {string[]} argv - command line arguments
 */
async function main(argv) {
  const args = argv.slice(2)

  // log command explores migration logfile
  if (args[0] === 'log') {
    return await migrationLogCli(...args.slice(1))
  }

  // <space.did> store/add --link {cid}
  if ('store/add' === args[1]) {
    const space = DID.match({ method: 'key' }).from(args[0])
    const flags = args.slice(2)
    return await migratePartCli(space, flags)
  }

  const { values } = parseArgs({
    args,
    options: {
      ipfs: {
        type: 'string',
        help: 'URL of IPFS gateway to use to resolve Upload part CIDs',
        default: 'https://w3s.link'
      },
      space: {
        type: 'string',
        help: 'space DID to migrate to',
      },
      'expect-store-add-status': {
        type: 'string',
        help: "If pesent, migration with stop with error if it encounters a store/add response whose status is not this value.  ",
      },
      w3up: {
        type: 'string',
        help: 'URL of w3up API to connect to',
        default: 'https://up.web3.storage',
      },
      log: {
        type: 'string',
        help: 'path to file to log migration events to',
      }
    },
  })

  /** @type {URL} */
  let w3upUrl
  try { w3upUrl = new URL(values.w3up) }
  catch (error) {
    throw new Error('unable to parse w3up option as URL', { cause: error })
  }

  const agent = await getDefaultW3upAgent(w3upUrl)

  // source of uploads is stdin by default
  // except stdin won't work if nothing is piped in.
  // If nothing piped in, ask the user what to do.
  const isInteractive = process.stdin.isTTY
  let source = isInteractive
    ? await getUploadsFromPrompts()
    : new W32023UploadsFromNdjson(Readable.toWeb(process.stdin))

  let spaceValue = values.space
    // if interactive, we can use env vars and check for confirmation
    ?? (isInteractive ? (process.env.W3_SPACE ?? process.env.WEB3_SPACE) : undefined)
  let spaceValueConfirmed
  if (spaceValue && isInteractive) {
    spaceValueConfirmed = await confirm({ message: `migrate to destination space ${spaceValue}?` })
    if (!spaceValueConfirmed) {
      spaceValue = undefined
    }
  }
  if (isInteractive && !spaceValue) {
    const chosenSpace = await promptForSpace(w3upUrl)
    console.warn('using space', chosenSpace.did())
    spaceValue = chosenSpace.did()
  }
  if (!spaceValue) {
    throw new Error(`Unable to determine migration destination. Will not migrate.`)
  }
  const space = DID.match({ method: 'key' }).from(spaceValue)

  // write ndjson events here
  const ndJsonLog = values.log ? createWriteStream(values.log) : undefined
  const migrationAbort = new AbortController
  const migration = migrate({
    signal: migrationAbort.signal,
    issuer: agent.issuer,
    w3up: agent.connection,
    source: Readable.toWeb(Readable.from(source)),
    destination: new URL(space),
    async fetchPart(cid, { signal }) {
      return await fetch(new URL(`/ipfs/${cid}`, values.ipfs), { signal })
    },
    onStoreAddReceipt(receipt) {
      const expectedStatus = values['expect-store-add-status']
      if (expectedStatus && (receipt.out.ok.status !== expectedStatus)) {
        throw Object.assign(new Error('unexpected store/add receipt'), { receipt })
      }
    },
    authorization: agent.proofs([
      {
        can: 'store/add',
        with: space,
      },
      {
        can: 'upload/add',
        with: space,
      },
    ])
  })

  const sourceLength = 'length' in source ? await source.length : undefined
  let uploadMigrationFailureCount = 0
  let uploadMigrationSuccessCount = 0
  const start = new Date
  const getProgressMessage = () => {
    const progress = `${uploadMigrationSuccessCount}/${sourceLength}`
    const percent = (uploadMigrationSuccessCount / sourceLength)
    const durationMs = Number(new Date) - Number(start)
    const etaMs = durationMs / percent
    const etaSeconds = etaMs / 1000
    const etaMinutes = etaSeconds / 60
    return `migrating to w3up… Uploads:${progress} ${uploadMigrationFailureCount ? `Failures:${uploadMigrationFailureCount} ` : ``}ETA:${etaMinutes.toFixed(1)}min`
  }
  const ui = isInteractive ? new inquirer.ui.BottomBar() : undefined
  for await (const event of migration) {
    // write ndjson to log file, if there is one
    ndJsonLog?.write(JSON.stringify(event, stringifyForMigrationProgressStdio) + '\n')

    if (event instanceof UploadMigrationFailure) {
      uploadMigrationFailureCount++
      // write failures to stderr
      console.warn(JSON.stringify(event, stringifyForMigrationProgressStdio, isInteractive ? 2 : undefined))
    } else {
      uploadMigrationSuccessCount++
      const space = event.add.receipt.ran.capabilities[0].with
      const root = event.add.receipt.ran.capabilities[0].nb.root
      const consoleLink = `https://console.web3.storage/space/${space}/root/${root}`
      if (ui) {
        ui?.log.write(consoleLink)
      }
    }
    ui?.updateBottomBar(getProgressMessage() + '\n')
  }
  if (uploadMigrationFailureCount) {
    console.warn(`failed to migrate ${uploadMigrationFailureCount}/${uploadMigrationSuccessCount + uploadMigrationFailureCount} uploads`)
    process.exit(1)
  } else {
    console.warn(`migrated ${uploadMigrationSuccessCount + uploadMigrationFailureCount} uploads`)
    // without this, process will hang at end of successful migration.
    // I think the following line indicates that the process hangs
    // due to tcp sockets still open to the server fetched via `options.fetchPart` passed to migrate() above.
    // A fix is probably to refactor how migration works to not pass around things that reference the response/socket indefinitely.
    // console.log('process._getActiveHandles()', process._getActiveHandles())
    process.exit(0)
  }
}

/**
 * cli for 'store add' command.
 * should get space DID from --space and CID from --link and then invoke store/add on the space
 * @param {import("@web3-storage/access").SpaceDID} spaceDid - did of space to add to
 * @param {string[]} args - cli flags to parse
 */
async function migratePartCli(spaceDid, args) {
  const { values } = parseArgs({
    args,
    options: {
      link: {
        type: 'string',
        help: 'CID to migrate',
      },
      w3up: {
        type: 'string',
        help: 'URL of w3up API to connect to',
        default: 'https://up.web3.storage',
      },
    },
  })
  /** @type {URL} */
  let w3upUrl
  try { w3upUrl = new URL(values.w3up) }
  catch (error) {
    throw new Error('unable to parse w3up option as URL', { cause: error })
  }
  const agent = await getDefaultW3upAgent(w3upUrl)
  const authorization = agent.proofs([{ can: 'store/add', with: spaceDid }])
  const add = Store.add.invoke({
    issuer: agent.issuer,
    audience: agent.connection.id,
    with: spaceDid,
    nb: carPartToStoreAddNb({
      part: stringToCarCid(values.link).toString(),
      response: await fetch(`https://w3s.link/ipfs/${values.link}`),
    }),
    proofs: authorization,
  })
  // @ts-expect-error agent.connection has no service type
  const receipt = await add.execute(agent.connection)
  console.log(JSON.stringify(receipt.out, undefined, 2))
}

/**
 * JSON.stringify replacer for progress of migration
 * @param {string} key - json property name
 * @param {any} value - json property value
 */
function stringifyForMigrationProgressStdio(key, value) {
  if (value instanceof Map) {
    return Object.fromEntries(value.entries())
  }
  if (value instanceof W32023Upload) {
    return (new W32023UploadSummary(value)).toJSON()
  }
  if ((value instanceof Error) && (!('toJSON' in value) || typeof value.toJSON !== 'function')) {
    return {
      name: value.name,
      message: value.message,
      cause: value.cause,
    }
  }
  return value
}

/**
 * get a Space by using interactive cli prompts using inquirer
 * @param {URL} w3upUrl - url to w3up
 */
async function promptForSpace(w3upUrl) {
  const w3up = await getDefaultW3up(w3upUrl)

  const selection = await select({
    message: 'choose a space',
    pageSize: 32,
    choices: w3up.spaces().map(s => {
      return {
        name: [s.name, s.did()].filter(Boolean).join(' - '),
        value: s,
        description: JSON.stringify(s.meta)
      }
    })
  })
  return selection
}

/**
 * get a stream of w32023 uploads via
 * interactive prompts using inquirer
 * + old web3.storage client library
 * @returns {Promise<AsyncIterable<W32023Upload> & { length: Promise<number> }>} uploads
 */
async function getUploadsFromPrompts() {
  const confirmation = await confirm({
    message: 'no uploads were piped in. Do you want to migrate uploads from old.web3.storage?',
  })
  if (!confirmation) throw new Error('unable to find a source of uploads to migrate')
  const envToken = process.env.WEB3_TOKEN
  let token;
  if (envToken && await confirm({ message: 'found WEB3_TOKEN in env. Use that?' })) {
    token = envToken
  } else {
    token = await promptForPassword({
      message: 'enter API token for old.web3.storage',
    })
  }
  const oldW3 = new Web3Storage({ token })
  const uploads = (async function* () {
    for await (const u of oldW3.list()) {
      if (u) {
        yield new W32023Upload(u)
      }
    }
  }())
  // get count
  const userUploadsResponse = fetch(`https://api.web3.storage/user/uploads`, {
    headers: { authorization: `Bearer ${token}` },
  })
  const count = userUploadsResponse.then(r => parseInt(r.headers.get('count'), 10)).then(c => isNaN(c) ? undefined : c)
  return Object.assign(uploads, { length: count })
}

/**
 * cli for `migrate-to-w3up log ` ...
 * `migrate-to-w3up log uploads-from-failures` should extract uploads from UploadMigrationFailure events in the log
 *   and log them to stdout.
 * @param {string[]} args - command line arguments
 */
async function migrationLogCli(...args) {
  const { values, positionals } = parseArgs({
    args,
    allowPositionals: true,
  })
  const command = positionals[0]
  switch (command) {
    case 'get-uploads-from-failures': {
      const logfile = positionals[1]
      if (!logfile) throw new Error(`provide a logfile path as larg arg`)
      for await (const event of readNDJSONStream(Readable.toWeb(createReadStream(logfile)))) {
        if (event.type === 'UploadMigrationFailure' && event.upload) {
          console.log(JSON.stringify(event.upload, stringifyForMigrationProgressStdio))
        }
      }
      return
    }
  }
  throw new Error(`unknown log subcommand: ${command}. Try 'get-uploads-from-failures'`)
}
