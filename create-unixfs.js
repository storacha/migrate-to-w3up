import { Writable } from 'node:stream'
import * as UnixFS from '@ipld/unixfs'
import { Web3Storage } from 'web3.storage'
import * as Link from 'multiformats/link'
import { CARWriterStream } from 'carstream/writer'

const token = process.env.WEB3_STORAGE_TOKEN ?? ''
const storage = new Web3Storage({ token })

const { readable, writable } = new TransformStream()
const writer = UnixFS.createWriter({ writable })
const dir = UnixFS.createShardedDirectoryWriter(writer)

let i = 0
let halt = false
process.on('SIGINT', () => {
  if (i === 0 || halt) process.exit()
  process.stderr.write('\nCtrl+C detected, halting...')
  halt = true
})

process.stderr.write('creating UnixFS directory')
for await (const item of storage.list()) {
  dir.set(`${item.created} ${item.name ?? `Upload #${i + 1}`}`, {
    cid: Link.parse(item.cid),
    contentByteLength: 0,
    dagByteLength: item.dagSize,
  })
  i++
  if (i % 100 === 0) process.stderr.write('.')
  if (halt) break
}
console.error()

readable
  .pipeThrough(new CARWriterStream())
  .pipeTo(Writable.toWeb(process.stdout))

const root = await dir.close()
console.error(`✅ successfully created UnixFS directory with root ${root.cid}`)
if (halt) console.error('⚠️ process was interrupted, UnixFS directory does NOT contain complete listing')
