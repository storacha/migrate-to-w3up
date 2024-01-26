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
import {DID} from "@ucanto/validator"
import { StoreConf } from '@web3-storage/access/stores/store-conf'

/**
 * @typedef {import('@ucanto/interface').Delegation[]} Authorization
 */

/**
 * @typedef {object} AddUploadEvent
 */

/**
 * @typedef {object} AddCarEvent
 */

/**
 * @typedef {AddUploadEvent|AddCarEvent} MigrationEvent
 */

/**
 * @param {object} options 
 * @param {import("@ucanto/client").SignerKey} options.issuer
 * @param {Authorization} [options.authorization]
 * @param {import("@ucanto/client").ConnectionView} options.w3up
 * @param {AsyncIterable<W32023Upload>} options.source
 * @param {URL} options.destination - e.g. a space DID
 * @param {AbortSignal} [options.signal]
 */
export async function * migrate({
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
          console.warn('successfully invoked store/add with link=', add.nb.link)
        } else {
          console.warn('receipt.out', receipt.out)
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
        // @ts-ignore
        switch (receipt.out.ok.status) {
          case "done":
            console.warn(`store/add ok indicates car ${add.part} was already in w3up`)
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
                // @ts-ignore
                duplex: 'half' 
              }
            )
            const sendToPresignedResponse = await fetch(sendCarRequest, { signal })
            // ensure was 2xx, otherwise throw because something unusual happened
            if ( ! (200 <= sendToPresignedResponse.status && sendToPresignedResponse.status < 300)) {
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
            // @ts-ignore
            throw new Error(`unexpected store/add ok.status: ${receipt.out.ok.status}`)
            // next part
            continue
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
      if ( ! uploadAddReceipt.out.ok) {
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

const isMain = (url, argv=process.argv) => fileURLToPath(url) === fs.realpathSync(argv[1])
if (isMain(import.meta.url, process.argv)) {
  main(process.argv).catch(error => console.error('error in main()', error))
}

async function getDefaultW3up() {
  // @todo - we shouldn't need to reuse this store, though it's conventient for w3cli users.
  // instead, accept accept W3_PRINCIPAL and W3_PROOF env vars or flags 
  const store = new StoreConf({ profile: process.env.W3_STORE_NAME ?? 'w3cli' })
  const w3 = await w3up.create({ store })
  return w3
}

async function getDefaultW3upAgent() {
  const w3 = await getDefaultW3up()
  // @ts-expect-error _agent is protected property
  const access = w3._agent
  return access
}

import confirm from '@inquirer/confirm';
import { input } from '@inquirer/prompts';
import {Web3Storage} from 'web3.storage'
import promptForPassword from '@inquirer/password';

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
  if ( ! spaceValue) {
    const chosenSpace = await promptForSpace()
    console.log('using space', chosenSpace.did())
    spaceValue = chosenSpace.did()
  }
  const space = DID.match({ method: 'key' }).from(spaceValue)
  const agent = await getDefaultW3upAgent()
  // source of uploads is stdin by default
  let source
  // except stdin won't work if nothing is piped in.
  // If nothing piped in, ask the user what to do.
  if ( ! process.stdin.isTTY) {
    source = new W32023UploadsFromNdjson(Readable.toWeb(process.stdin))
  } else {
    source = await getUploadsFromPrompts()
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
    console.log(JSON.stringify(event))
  }
}

import { select } from '@inquirer/prompts';

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

async function getUploadsFromPrompts() {
  const confirmation = await confirm({
    message: 'no uploads were piped in. Do you want to migrate uploads from old.web3.storage?',
  })
  if ( ! confirmation) throw new Error('unable to find a source of uploads to migrate')
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
