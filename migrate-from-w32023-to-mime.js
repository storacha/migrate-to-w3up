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
import {Base64Encode} from 'base64-stream';

const isMain = (url, argv=process.argv) => fileURLToPath(url) === fs.realpathSync(argv[1])
if (isMain(import.meta.url, process.argv)) {
  main(process.argv).catch(error => console.error('error in main()', error))
}

function getContentTypeFromCid(cid) {
  const parsed = CID.parse(cid)
  switch (parsed.code) {
    case 0x70:
      return 'application/vnd.ipld.dag-pb'
    default:
      throw new Error(`unexpected code ${parsed.code}`)
  }
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
    },
    fetchParts: {
      type: 'boolean',
      default: false,
      help: 'whether to fetch parts for each upload and include those in the multipart output',
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
          fetchParts: args.values.fetchParts,
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
 * @param {boolean} [options.fetchParts] - whether to fetch part CID for each upload
 * @param {(object) => Promise<void>} [options.forEachUpload]
 * @param {(object) => string} [options.getPartContentType] - get content type for a single part
 * @param {(object) => object} [options.getPartHeaders] - get headers for a single part
 */
async function createMultipartRelatedReadable(ndjsonUploads, options={}) {
  const { type = 'Multipart/Mixed' } = options
  const uploadsMultipart = new Multipart()
  /** @type {Promise<FetchedUploadPart[]>[]} */
  const queueToFetchUploadParts = []
  const fetchUploadParts = async (upload) => {
    const fetchedParts = await Promise.all(upload.parts.map(async cid => {
      const url = new URL(`https://w3s.link/ipfs/${cid}`)
      const response = await fetch(url);
      return {
        upload,
        url,
        response,
      }
    }))
    return fetchedParts
  }
  for await (const object of readNDJSONStream(ndjsonUploads)) {
    await options?.forEachUpload?.(object)
    const body = JSON.stringify(object, undefined, 2) + '\n'
    const bodyDagJsonCid = CID.create(1, dagJson.code, await sha256.digest(dagJson.encode(object)))
    const contentType = options?.getPartContentType?.(object);
    uploadsMultipart.addPart({
      headers: {
        ...(options?.getPartHeaders(object) ?? {}),
        ...(contentType ? { 'content-type': contentType } : {}),
        'content-id': `${bodyDagJsonCid}`,
      },
      body,
    })

    if (options.fetchParts) {
      queueToFetchUploadParts.push(fetchUploadParts(object))
    }
  }
  while (queueToFetchUploadParts.length) {
    const fetchedUploads = await (queueToFetchUploadParts.pop())
    for (const { upload, response, url } of fetchedUploads) {
      console.warn({ upload, response, url })
      uploadsMultipart.addPart({
        headers: {
          'content-id': upload.cid,
          'content-type': getContentTypeFromCid(upload.cid),
          'content-transfer-encoding': 'BASE64'
        },
        body: Readable.fromWeb(response.body).pipe(new Base64Encode),
      })
    }
  }
  const multipartUploads = () => Readable.from(async function * () {
    function * text (text) { yield new TextEncoder().encode(text) }
    yield * text(`Content-Type: ${type}`)
    yield * text(`; boundary=${uploadsMultipart.boundary}`)
    if (options?.type) {
      yield * text(`; type=${options.type}`)
    }
    yield * text('\n\n')
    yield * new ReadableStream({
      async start(controller) {
        uploadsMultipart.on('data', chunk => {
          controller.enqueue(chunk)
        })
      }
    })
  }())
  return multipartUploads()
}
