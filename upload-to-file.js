#!/usr/bin/env node

import fs from 'fs'
import { fileURLToPath } from 'url'
import { parseArgs } from 'util'
import { fetchUploadParts } from './upload.js'
import { recursive as exporter } from 'ipfs-unixfs-exporter'
// import * as pb from '@ipld/dag-pb'
import * as stream from 'node:stream'
import { CarReader } from '@ipld/car'
import readNDJSONStream from 'ndjson-readablestream';

const isMain = (url, argv=process.argv) => fileURLToPath(url) === fs.realpathSync(argv[1])
if (isMain(import.meta.url, process.argv)) {
  main(process.argv).catch(error => console.error('error in main()', error))
}

/**
 * accept upload json as input, and emit a file decoded from it
 * @param {string[]} argv - command line arguments
 */
async function main(argv) {
  const args = parseArgs({
    args: argv.slice(2),
    options: {
      from: {
        type: 'string',
        default: '/dev/stdin',
        help: 'where to get data from'
      },
    },
  })
  for await (const upload of readNDJSONStream(stream.Readable.toWeb(fs.createReadStream(args.values.from)))) {
    const parts = await fetchUploadParts(upload)
    if (parts.length !== 1) {
      throw new Error(`expected 1 part but got ${parts.length}`)
    }
    const part = parts[0]
  
    await fetchPartAndSaveToDisk(part.response)
  }
}

/**
 * fetch a part and save it to disk
 * @param {Response} part - http response of CAR
 */
async function fetchPartAndSaveToDisk(part) {
  // @ts-expect-error readablestream types are weird
  const reader = await CarReader.fromIterable(part.body)
  const roots = await reader.getRoots()
  for (const root of roots) {
    // const readRoot = await reader.get(root)
    // const decoded = pb.decode(readRoot.bytes)
    const entries = exporter(root, {
      async get (cid) {
        const block = await reader.get(cid)
        return block.bytes
      }
    })
    for await (const entry of entries) {
      if (entry.type === 'file' || entry.type === 'raw') {
        const contentReadable = stream.Readable.from(entry.content())
        const entryFile = fs.createWriteStream(entry.path)
        await stream.pipeline(contentReadable, entryFile, (err) => {
          // console.log('pipeline end', { err })
        })
        console.warn('wrote', entry.path)
      } else if (entry.type === 'directory') {
        fs.mkdirSync(entry.path, { recursive: true })
      }
    }
  }
}
