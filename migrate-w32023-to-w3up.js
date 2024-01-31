#!/usr/bin/env node
import { W32023Upload, W32023UploadSummary, W32023UploadsFromNdjson } from "./w32023.js";
import * as Link from 'multiformats/link'
import { fileURLToPath } from 'url'
import fs from 'fs'
import { Readable } from 'node:stream'
import { toStoreAdd } from "./w32023-to-w3up.js";
import * as w3up from "@web3-storage/w3up-client"
import { parseArgs } from 'node:util'
import { Store, Upload } from '@web3-storage/capabilities'
import { DID } from "@ucanto/validator"
import { StoreConf } from '@web3-storage/access/stores/store-conf'
import { select } from '@inquirer/prompts';
import { Parallel } from 'parallel-transform-web'

/**
 * @typedef {import('@ucanto/interface').Delegation[]} Authorization
 */

/**
 * a single part of an upload, with the part fetchable
 * @typedef FetchableUploadPart
 * @property {string} part - cid of car part
 * @property {W32023Upload} upload - upload that has part in .parts
 * @property {(options?:{signal?:AbortSignal}) => Promise<Response>} fetch - fetch the car bytes
 */

/**
 * a single part of an upload, with the part fetched
 * @typedef UploadPartWithResponse
 * @property {string} part - cid of car part
 * @property {W32023Upload} upload - upload that has part in .parts
 * @property {Response} response - result of fetching part
 */

/**
 * @typedef {object} AddUploadEvent
 * @property {{
 *   type: 'Upload'
 * }} object - object that was added to the migration destination
 */

/**
 * @typedef {object} AddCarEvent
 * @property {{
 *   type: 'car'
 * }} object - object that was added to the migration destination
 */

/**
 * @typedef {AddUploadEvent|AddCarEvent} MigrationEvent
 */

/**
 * @param {W32023Upload} upload - upload with parts
 * @param {(part: string, options?: { signal?: AbortSignal }) => Promise<Response>} fetchPart - given a part CID, fetch it and return a Response
 * @yields {FetchableUploadPart} for each part in upload.parts
 */
const transformUploadToFetchableUploadPart = async function* (upload, fetchPart) {
  for (const part of upload.parts) {
    /** @type {FetchableUploadPart} */
    const withResponse = {
      upload,
      part,
      fetch({ signal }) {
        return fetchPart(part, { signal })
      },
    }
    yield withResponse
  }
}

/**
 * @param {W32023Upload} upload - upload with parts to ensure we have responses for each of
 * @param {Map<string, Map<string, any>>} uploadCidToParts - Map<upload.cid, Map<part.cid, unknown>> - where to store state while waiting for all parts of an upload
 */
function receivedAllUploadParts(upload, uploadCidToParts) {
  const partsReceived = uploadCidToParts.get(upload.cid)
  for (const part of upload.parts) {
    if (!partsReceived.has(part)) {
      return false
    }
  }
  return true
}

/**
 * @implements {Transformer<
 *   MigratedUploadOnePart,
 *   MigratedUploadAllParts
 * >}
 */
class CollectMigratedUploadParts {
  /**
   * @param {AbortSignal} [signal] - emits event when this transformer should abort
   */
  constructor(signal) {
    this.signal = signal
  }
  /**
   * @param {MigratedUploadOnePart} input - input for each part in upload.parts
   * @param {TransformStreamDefaultController} controller - enqueue output here
   */
  async transform(input, controller) {
    try {
      for await (const output of collectMigratedParts(input, this)) {
        controller.enqueue(output)
      }
    } catch (error) {
      console.warn('error in CollectMigratedUploadParts', error)
      throw error
    }
  }
}

/**
 * transform each upload part with fetched response,
 * collect all parts for an upload,
 * then yield { upload, parts }.
 * If you want low memory usage, be sure to order inputs by upload.
 * @param {MigratedUploadOnePart} migratedPart - input to transform
 * @param {object} options - options
 * @param {AbortSignal} [options.signal] - for cancelling the migration
 * @param {Map<string, Map<string, MigratedUploadOnePart>>} [options.uploadCidToParts] - Map<upload.cid, Map<part.cid, { response }>> - where to store state while waiting for all parts of an upload
 * @yields {MigratedUploadAllParts} upload with all parts
 */
const collectMigratedParts = async function* (
  migratedPart,
  {
    signal,
    uploadCidToParts = new Map
  } = {}
) {
  signal?.throwIfAborted()
  const {
    upload,
  } = migratedPart
  if (!uploadCidToParts.has(upload.cid)) {
    uploadCidToParts.set(upload.cid, new Map)
  }
  const partsForUpload = uploadCidToParts.get(upload.cid)
  if (!partsForUpload) throw new Error(`unexpected falsy parts`)
  partsForUpload.set(migratedPart.part, migratedPart)

  if (receivedAllUploadParts(upload, uploadCidToParts)) {
    // console.debug('we have all car parts!', {
    //   partsForUpload,
    //   'upload.parts': upload.parts,
    // })
    // no need to keep this memory around
    uploadCidToParts.delete(upload.cid)
    signal?.throwIfAborted()
    /** @type {MigratedUploadAllParts} */
    const allparts = {
      upload,
      parts: partsForUpload,
    }
    yield allparts
  } else {
    // console.debug('still waiting for car parts', {
    //   'partsForUpload.size': partsForUpload.size,
    //   'upload.parts.length': upload.parts.length,
    // })
  }
  signal?.throwIfAborted()
}

/**
 * @implements {Transformer<
 *   W32023Upload,
 *   FetchableUploadPart
 * >}
 */
class UploadToFetchableUploadPart {
  /**
   * @param {object} options - options
   * @param {(part: string, options?: { signal?: AbortSignal }) => Promise<Response>} options.fetchPart - given a part CID, return the fetched response
   */
  constructor({ fetchPart }) {
    this.fetchPart = fetchPart
  }
  /**
   * @param {W32023Upload} upload - upload to transform into one output per upload.part
   * @param {TransformStreamDefaultController} controller - enqueue output her
   */
  async transform(upload, controller) {
    for await (const out of transformUploadToFetchableUploadPart(upload, this.fetchPart)) {
      controller.enqueue(out)
    }
  }
}

/**
 * @param {UploadPartWithResponse} options - options
 */
function carPartToStoreAddNb(options) {
  const carSizeString = options.response.headers.get('content-length')
  const carSize = carSizeString && parseInt(carSizeString, 10)
  if (!carSize) {
    throw new Error(`unable to determine carSize for response to ${options.response.url}`)
  }
  const link = Link.parse(options.part)
  /** @type {import("@web3-storage/access").StoreAdd['nb']} */
  const addNb = {
    link,
    size: carSize
  }
  return addNb
}

/**
 * @param {import("@web3-storage/access").StoreAddSuccess} storeAddSuccess - successful store/add result
 * @param {ReadableStream<Uint8Array>} car - car bytes
 * @param {object} [options] - options
 * @param {AbortSignal} [options.signal] - emits when this should abort
 */
async function uploadBlockForStoreAddSuccess(
  storeAddSuccess,
  car,
  options = {}
) {
  switch (storeAddSuccess.status) {
    case "done":
      // no work needed
      return
    case "upload":
      break;
    default:
      // @ts-expect-error storeAddSuccess could be never type, but in practice something else
      throw new Error(`unexpected store/add success status: "${storeAddSuccess.status}"`)
  }
  // need to do upload
  const sendCarRequest = new Request(
    storeAddSuccess.url,
    {
      method: 'PUT',
      mode: 'cors',
      headers: storeAddSuccess.headers,
      body: car,
      redirect: 'follow',
      // @ts-expect-error not in types, but required for the body to work
      duplex: 'half'
    }
  )
  const sendToPresignedResponse = await fetch(sendCarRequest, { signal: options.signal })
  // ensure was 2xx, otherwise throw because something unusual happened
  if (!(200 <= sendToPresignedResponse.status && sendToPresignedResponse.status < 300)) {
    console.warn('unsuccessful sendToPresignedResponse', sendToPresignedResponse)
    throw Object.assign(
      new Error(`error sending car bytes to url from store/add response`), {
      response: sendToPresignedResponse,
    }
    )
  }
  return sendToPresignedResponse
}

/**
 * a single block that has been migrated to w3up.
 * i.e. it has a store/add receipt.
 * if the receipt instructed the client to send car bytes, that already happened too
 */
class MigratedUploadOnePart {
  /** @type {W32023Upload} */
  upload
  /** @type {string} */
  part
  /**
   * @type {{
   *   invocation: unknown
   *   receipt: import('@ucanto/interface').Receipt<import("@web3-storage/access").StoreAddSuccess>
   * }}
   */
  add
  /**
   * @type {undefined|{
   *   response: Response
   * }}
   */
  copy
}

/**
 * a single upload with all blocks migrated to w3up.
 */
class MigratedUploadAllParts {
  /** @type {W32023Upload} */
  upload
  /**
   * map of part CID to migrated part block
   * @type {Map<string, MigratedUploadOnePart>}
   */
  parts
}

/**
 * @param {object} options - options
 * @param {import("@ucanto/client").SignerKey} options.issuer - principal that will issue w3up invocations
 * @param {Authorization} [options.authorization] - authorization sent with w3up invocations
 * @param {import("@ucanto/client").ConnectionView} options.w3up - connection to w3up on which invocations will be sent
 * @param {URL} options.destination - e.g. w3up space DID to which source uploads will be migrated
 * @param {AbortSignal} [options.signal] - for cancelling the migration
 * @param {ReadableStream<W32023Upload>} options.source - uploads that will be migrated from w32023 json format to w3up
 * @param {number} [options.concurrency] - max concurrency for any phase of pipeline
 * @param {(part: string, options?: { signal?: AbortSignal }) => Promise<Response>} options.fetchPart - given a part CID, return the fetched response
 */
export async function* migrateWithConcurrency({
  issuer,
  authorization,
  w3up,
  destination,
  signal,
  source,
  concurrency = 1,
  fetchPart,
}) {
  if (concurrency < 1) {
    throw new Error(`concurrency must be at least 1`)
  }
  const space = DID.match({ method: 'key' }).from(destination.toString())
  const results = source
    .pipeThrough(new TransformStream(
      new UploadToFetchableUploadPart({ fetchPart }),
    ))
    // make sure that the block for the store/add invocation
    // has been migrated fully to w3up.
    .pipeThrough(
      new Parallel(
        concurrency,
        /**
         * @param {FetchableUploadPart} fetchablePart - upload to transform
         * @returns {Promise<MigratedUploadOnePart>} - migrated car part
         */
        async function (fetchablePart) {
          signal?.throwIfAborted()
          const partFetchResponse = await fetchablePart.fetch({ signal })
          const addNb = carPartToStoreAddNb({ ...fetchablePart, response: partFetchResponse })
          const invocation = Store.add.invoke({
            issuer,
            audience: w3up.id,
            proofs: authorization,
            with: space,
            nb: addNb,
          })
          const receipt = await invocation.execute(w3up)
          const storeAddSuccess = receipt.out.ok
          const copyResponse = storeAddSuccess && await uploadBlockForStoreAddSuccess(
            // @ts-expect-error no svc type
            receipt.out.ok,
            partFetchResponse,
          )
          /**
           * @type {MigratedUploadOnePart}
           */
          const output = {
            ...fetchablePart,
            add: {
              invocation,
              // @ts-expect-error - receipt has no service type to guarantee StoreAddSuccess
              receipt,
            },
            copy: copyResponse && {
              response: copyResponse,
            },
          }
          return output
        }
      )
    )
    .pipeThrough(new TransformStream(new CollectMigratedUploadParts))
    // todo still need to invoke upload/add

  const reader = results.getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) {
      return;
    }
    yield value
  }
}

/**
 * @param {object} options - options
 * @param {import("@ucanto/client").SignerKey} options.issuer - principal that will issue w3up invocations
 * @param {Authorization} [options.authorization] - authorization sent with w3up invocations
 * @param {import("@ucanto/client").ConnectionView} options.w3up - connection to w3up on which invocations will be sent
 * @param {AsyncIterable<W32023Upload>} options.source - uploads that will be migrated from w32023 json format to w3up
 * @param {URL} options.destination - e.g. w3up space DID to which source uploads will be migrated
 * @param {AbortSignal} [options.signal] - for cancelling the migration
 * @yields {MigrationEvent} - informative events are sent after a step in the migration
 */
export async function* migrate({
  issuer,
  authorization,
  w3up,
  source,
  destination,
  signal,
}) {
  signal?.throwIfAborted()
  const space = DID.match({ method: 'key' }).from(destination.toString())
  for await (const upload of source) {
    signal?.throwIfAborted()
    for await (const add of toStoreAdd(upload)) {
      signal?.throwIfAborted()
      const storeAdd = {
        with: space,
        nb: add.nb,
      }
      const receipt = await Store.add.invoke({
        issuer,
        audience: w3up.id,
        proofs: authorization,
        with: space,
        nb: add.nb,
      }).execute(
        w3up,
      )
      if (receipt.out.ok) {
        // console.warn('successfully invoked store/add with link=', add.nb.link)
      } else {
        // console.warn('receipt.out', receipt.out)
        throw Object.assign(
          new Error(`failure invoking store/add`),
          {
            add,
            with: space,
            receipt: {
              ...receipt,
              out: receipt.out,
            },
          },
        )
      }
      let copiedCarTo
      // we know receipt indicated successful store/add.
      // now let's upload the car bytes if the response hints we should
      // @ts-expect-error service types imperfect, but this should be resilient to unexpected ok types
      switch (receipt.out?.ok?.status) {
        case "done":
          // console.warn(`store/add ok indicates car ${add.part} was already in w3up`)
          break;
        case "upload": {
          // we need to upload car bytes
          const carResponse = await fetch(add.partUrl, { signal })
          // fetch car bytes
          /** @type {any} */
          const storeAddSuccess = receipt.out.ok

          if (carResponse.status !== 200) {
            throw Object.assign(
              new Error(`unexpected non-200 response status code when fetching car from w3s.link`),
              { part: add.part, url: add.partUrl, response: carResponse },
            )
          }
          // carResponse has status 200
          if (carResponse.headers.has('content-length')) {
            console.warn(`car ${add.part} has content-length`, carResponse.headers.get('content-length'))
          }
          console.warn(`piping CAR bytes from ${add.partUrl} to url from store/add response "${storeAddSuccess.url}"`)
          const sendCarRequest = new Request(
            storeAddSuccess.url,
            {
              method: 'PUT',
              mode: 'cors',
              headers: storeAddSuccess.headers,
              body: carResponse.body,
              redirect: 'follow',
              // @ts-expect-error not in types, but required for the body to work
              duplex: 'half'
            }
          )
          const sendToPresignedResponse = await fetch(sendCarRequest, { signal })
          // ensure was 2xx, otherwise throw because something unusual happened
          if (!(200 <= sendToPresignedResponse.status && sendToPresignedResponse.status < 300)) {
            console.warn('unsuccessful sendToPresignedResponse', sendToPresignedResponse)
            throw Object.assign(
              new Error(`error sending car bytes to url from store/add response`), {
              response: sendToPresignedResponse,
            }
            )
          }
          copiedCarTo = {
            request: sendCarRequest,
            response: sendToPresignedResponse,
          }
          break;
        }
        default:
          console.warn('unexpected store/add ok.status', receipt.out.ok)
          // @ts-expect-error receipt from service with imprecise types
          throw new Error(`unexpected store/add ok.status: ${receipt.out?.ok?.status}`)
        // // next part
        // continue
      }
      // it's been added to the space. log that
      yield {
        type: 'Add',
        attributedTo: issuer.did(),
        source: new W32023UploadSummary(upload),
        object: {
          type: 'car',
          cid: add.nb.link.toString(),
          size: add.nb.size.toString(),
          ...(copiedCarTo && {
            copy: {
              request: {
                url: copiedCarTo.request.url.toString(),
                method: copiedCarTo.request.method.toString(),
                headers: copiedCarTo.request.headers,
              },
              response: {
                status: copiedCarTo.response.status,
              }
            }
          })
        },
        target: {
          type: 'Space',
          id: space,
        },
        invocation: storeAdd,
        receipt: {
          cid: receipt.root.cid.toString(),
          out: receipt.out,
        },
      }
    }
    // store/add is done for upload
    // need to do an upload/add
    const shards = upload.parts.map(c => Link.parse(c).toV1())
    const root = Link.parse(upload.cid)
    const uploadAddReceipt = await Upload.add.invoke({
      issuer,
      audience: w3up.id,
      proofs: authorization,
      with: space,
      nb: {
        root,
        // @ts-expect-error shards wants CAR links not any links
        shards,
      },
    }).execute(
      w3up,
    )
    if (!uploadAddReceipt.out.ok) {
      throw Object.assign(new Error(`failure result from upload/add invocation`), {
        result: uploadAddReceipt,
        out: uploadAddReceipt.out,
      })
    }
    yield {
      type: 'Add',
      attributedTo: issuer.did(),
      source: new W32023UploadSummary(upload),
      object: {
        type: 'Upload',
        root,
        shards,
      },
      target: {
        type: 'Space',
        id: space,
      },
      receipt: {
        cid: uploadAddReceipt.root.cid.toString(),
        out: uploadAddReceipt.out,
      },
    }
  }
}

const isMain = (url, argv = process.argv) => fileURLToPath(url) === fs.realpathSync(argv[1])
if (isMain(import.meta.url, process.argv)) {
  main(process.argv).catch(error => console.error('error in main()', error))
}

/**
 * get w3up-client with store from a good default store
 */
async function getDefaultW3up() {
  // @todo - we shouldn't need to reuse this store, though it's conventient for w3cli users.
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

import confirm from '@inquirer/confirm';
import { Web3Storage } from 'web3.storage'
import promptForPassword from '@inquirer/password';

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
  let spaceValue = values.space
  if (!spaceValue) {
    const chosenSpace = await promptForSpace()
    console.log('using space', chosenSpace.did())
    spaceValue = chosenSpace.did()
  }
  const space = DID.match({ method: 'key' }).from(spaceValue)
  const agent = await getDefaultW3upAgent()
  // source of uploads is stdin by default
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
  const migration = migrate({
    issuer: agent.issuer,
    w3up: agent.connection,
    source,
    destination: new URL(space),
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
    console.log(JSON.stringify(event, undefined, isInteractive ? 2 : undefined))
  }
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
 */
async function getUploadsFromPrompts() {
  const confirmation = await confirm({
    message: 'no uploads were piped in. Do you want to migrate uploads from old.web3.storage?',
  })
  if (!confirmation) throw new Error('unable to find a source of uploads to migrate')
  const envToken = process.env.WEB3_TOKEN
  let token;
  if (await confirm({ message: 'found WEB3_TOKEN in env. Use that?' })) {
    token = envToken
  } else {
    token = await promptForPassword({
      message: 'enter API token for old.web3.storage',
    })
  }
  const oldW3 = new Web3Storage({ token })
  return oldW3.list()
}
