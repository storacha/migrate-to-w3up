#!/usr/bin/env node
import { W32023Upload, W32023UploadsFromNdjson } from "./w32023.js";
import { fileURLToPath } from 'url'
import fs from 'fs'
import { Readable } from 'node:stream'
import * as w3up from "@web3-storage/w3up-client"
import { parseArgs } from 'node:util'
import { DID } from "@ucanto/validator"
import { StoreConf } from '@web3-storage/access/stores/store-conf'
import { select } from '@inquirer/prompts';
import confirm from '@inquirer/confirm';
import { Web3Storage } from 'web3.storage'
import promptForPassword from '@inquirer/password';
import { migrate } from "./migrate-w32023-to-w3up.js";
import { receiptToJson } from "./w3up-migration.js";

// if this file is being executed directly, run main() function
const isMain = (url, argv = process.argv) => fileURLToPath(url) === fs.realpathSync(argv[1])
if (isMain(import.meta.url, process.argv)) {
  main(process.argv).catch(error => console.error('error in main()', error))
}

/**
 * get w3up-client with store from a good default store
 */
async function getDefaultW3up() {
  // instead, accept accept W3_PRINCIPAL and W3_PROOF env vars or flags 
  const store = new StoreConf({ profile: process.env.W3_STORE_NAME ?? 'w3cli' })
  const w3 = await w3up.create({ store })
  return w3
}

/**
 * get a @web3-storage/access/agent instance with default store
 */
async function getDefaultW3upAgent() {
  const w3 = await getDefaultW3up()
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
  const { values } = parseArgs({
    options: {
      space: {
        type: 'string',
        help: 'space DID to migrate to',
      }
    }
  })

  const agent = await getDefaultW3upAgent()
  // source of uploads is stdin by default
  /** @type {AsyncIterable<W32023Upload>} */
  let source
  let isInteractive
  // except stdin won't work if nothing is piped in.
  // If nothing piped in, ask the user what to do.
  if (!process.stdin.isTTY) {
    source = new W32023UploadsFromNdjson(Readable.toWeb(process.stdin))
  } else {
    source = await getUploadsFromPrompts()
    isInteractive = true
  }

  let spaceValue = values.space
  if (isInteractive && !spaceValue) {
    const chosenSpace = await promptForSpace()
    console.warn('using space', chosenSpace.did())
    spaceValue = chosenSpace.did()
  }
  if ( ! spaceValue) {
    throw new Error(`pass migration destination with \`--space <space.did>\`.`)
  }
  const space = DID.match({ method: 'key' }).from(spaceValue)

  const migration = migrate({
    issuer: agent.issuer,
    w3up: agent.connection,
    source: Readable.toWeb(Readable.from(source)),
    destination: new URL(space),
    async fetchPart(cid, { signal }) {
      return await fetch(new URL(`/ipfs/${cid}`, 'https://w3s.link'), { signal })
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
  for await (const event of migration) {
    console.log(JSON.stringify(event, stringifyForMigrationProgressStdio, isInteractive ? 2 : undefined))
  }
}

/**
 * JSON.stringify replacer for progress of migration
 * @param {string} key - json property name
 * @param {any} value - json property value
 */
function stringifyForMigrationProgressStdio(key, value) {
  if (key === 'receipt' && value) {
    return receiptToJson(value)
  }
  if (value instanceof Map) {
    return Object.fromEntries(value.entries())
  }
  return value
}

/**
 * get a Space by using interactive cli prompts using inquirer
 */
async function promptForSpace() {
  const w3up = await getDefaultW3up()

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
 * @returns {Promise<AsyncIterable<W32023Upload>>} uploads
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
  const uploads = oldW3.list()
  return uploads
}
