#!/usr/bin/env node
/**
 * @fileoverview command line interface with tools for migrating from https://old.web3.storage/.
 */
import path from 'path'
import fs from 'fs'
import { fileURLToPath } from 'url'
import {parseArgs} from 'node:util'
import { Readable, Writable } from 'stream'
import readNDJSONStream from 'ndjson-readablestream';
import stream from 'node:stream'
import consumers from 'node:stream/consumers'
import * as Link from 'multiformats/link'
import { CID } from 'multiformats/cid'
import * as dagJson from 'multiformats/codecs/json'
import { sha256 } from 'multiformats/hashes/sha2'

const Multipart = await import('multipart-stream').then(msd => {
  return msd.default
})

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
      default: 'application/vnd.web3.storage.upload.v2023+ndjson',
      help: 'what kind of data to expect when sourced from options.from'
    },
    to: {
      type: 'string',
      help: 'where to write'
    }
  }
  const args = parseArgs({
    args: argv.slice(2),
    options,
  })
  const stdin = Readable.toWeb(fs.createReadStream(args.values.from))
  const uploadsMultipart = new Multipart()
  for await (const object of readNDJSONStream(stdin)) {
    const body = dagJson.encode(object)
    const bodyDagJsonCid = CID.create(1, dagJson.code, await sha256.digest(body))
    uploadsMultipart.addPart({
      headers: {
        'content-type': 'application/vnd.web3.storage.upload.v2023+json',
        'content-id': `ipfs:${bodyDagJsonCid}`
      },
      body,
    })
  }

  const to = args.values.to
  if ( ! to) {
    throw new Error(`provide --to=/path/to/output`)
  }
  const uploadsMultipartReadable = () => new ReadableStream({
    async start(controller) {
      uploadsMultipart.on('data', chunk => {
        controller.enqueue(chunk)
      })
    }
  })
  const header = () => Readable.from((async function * () {
    const text = function * (text) { yield new TextEncoder().encode(text) }
    yield * text('Content-Type: Multipart/Related')
    yield * text(`; boundary=${uploadsMultipart.boundary}`)
    yield * text(`; type=${args.values.fromMediaType}`)
    yield * text('\n\n')
  }()))
  const multipartUploads = () => Readable.from(async function * () {
    yield * header()
    yield * uploadsMultipartReadable()
  }())

  const writeTo = fs.createWriteStream(to)

  await stream.pipeline(multipartUploads(), writeTo, (err) => {
    throw err
  })
  console.warn('wrote', to)
}
