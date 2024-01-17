#!/usr/bin/env node
/**
 * @fileoverview command line interface with tools for migrating from https://old.web3.storage/.
 */
import fs from 'fs'
import { fileURLToPath } from 'url'
import {parseArgs} from 'node:util'
import { Readable } from 'stream'
import readNDJSONStream from 'ndjson-readablestream';
import stream from 'node:stream'
import { CID } from 'multiformats/cid'
import * as dagJson from 'multiformats/codecs/json'
import { sha256 } from 'multiformats/hashes/sha2'
const Multipart = await import('multipart-stream').then(m => m.default)

const isMain = (url, argv=process.argv) => fileURLToPath(url) === fs.realpathSync(argv[1])
if (isMain(import.meta.url, process.argv)) {
  main(process.argv).catch(error => console.error('error in main()', error))
}

async function main(argv) {
  const options = {
    from: {
      type: 'string',
      default: '/dev/stdin',
      help: 'where to get data from'
    },
    fromMediaType: {
      type: 'string',
      default: 'application/vnd.web3.storage.car+ndjson;version=2023.old.web3.storage',
      help: 'what kind of data to expect when sourced from options.from'
    },
    to: {
      type: 'string',
      help: 'where to write',
      default: '/dev/stdout',
    }
  }
  const args = parseArgs({
    args: argv.slice(2),
    options,
  })
  const toMediaType = args.values.fromMediaType.replace(/\+ndjson$/, '+json')
  let encoded
  switch (toMediaType) {
    case 'application/vnd.web3.storage.car+ndjson;version=2023.old.web3.storage':
      encoded = await createMultipartRelatedReadable(
        Readable.toWeb(fs.createReadStream(args.values.from)),
        {
          type: 'multipart/mixed',
          getPartContentType: () => args.values.fromMediaType.replace(/\+ndjson/, '+json'),
          getPartHeaders: (object) => {
            return {
              ...(object.name ? {
                'content-disposition': `attachment; filename="${object.name}.${object.cid}.ipfs"`,
              } : {})
            }
          }
        }
      )
      break;
    default:
      throw new Error(`unsupported target mediaType "${toMediaType}"`)
  }
  const to = args.values.to
  await stream.pipeline(encoded, fs.createWriteStream(to), (err) => {
    throw err
  })
  console.warn('wrote', to)
}

/**
 * @param {ReadableStream<Uint8Array>} ndjsonUploads
 * @param {object} [options]
 * @param {string} [options.type] - content-type of whole message
 * @param {(object) => string} [options.getPartContentType] - get content type for a single part
 * @param {(object) => object} [options.getPartHeaders] - get headers for a single part
 */
async function createMultipartRelatedReadable(ndjsonUploads, options={}) {
  const { type = 'Multipart/Mixed' } = options
  const uploadsMultipart = new Multipart()
  for await (const object of readNDJSONStream(ndjsonUploads)) {
    const body = JSON.stringify(object, undefined, 2) + '\n'
    const bodyDagJsonCid = CID.create(1, dagJson.code, await sha256.digest(dagJson.encode(object)))
    const contentType = options?.getPartContentType?.(object);
    uploadsMultipart.addPart({
      headers: {
        ...(options?.getPartHeaders(object) ?? {}),
        ...(contentType ? { 'content-type': contentType } : {}),
        'content-id': `ipfs:${bodyDagJsonCid}`,
      },
      body,
    })
  }
  const multipartRelatedHeader = () => Readable.from((async function * () {
    const text = function * (text) { yield new TextEncoder().encode(text) }
    yield * text(`Content-Type: ${type}`)
    yield * text(`; boundary=${uploadsMultipart.boundary}`)
    if (options?.type) {
      yield * text(`; type=${options.type}`)
    }
    yield * text('\n\n')
  }()))
  const parts = () => new ReadableStream({
    async start(controller) {
      uploadsMultipart.on('data', chunk => {
        controller.enqueue(chunk)
      })
    }
  })
  const multipartUploads = () => Readable.from(async function * () {
    yield * multipartRelatedHeader()
    yield * parts()
  }())
  return multipartUploads()
}
