import { Writable } from 'node:stream'
import { MemoryBlockstore } from '@web3-storage/pail/block'
import * as Batch from '@web3-storage/pail/batch'
import { ShardBlock } from '@web3-storage/pail/shard'
import { Web3Storage } from 'web3.storage'
import * as Link from 'multiformats/link'
import { CARWriterStream } from 'carstream/writer'

const token = process.env.WEB3_STORAGE_TOKEN ?? ''
const storage = new Web3Storage({ token })

const blocks = new MemoryBlockstore()
const genesis = await ShardBlock.create()
blocks.putSync(genesis.cid, genesis.bytes)

const batch = await Batch.create(blocks, genesis.cid)

let i = 0
let halt = false
process.on('SIGINT', () => {
  if (i === 0 || halt) process.exit()
  process.stderr.write('\nCtrl+C detected, halting...')
  halt = true
})

process.stderr.write('creating pail')
for await (const item of storage.list()) {
  await batch.put(`${item.created}/${item.name ?? `Upload #${i + 1}`}`, Link.parse(item.cid))
  i++
  if (i % 100 === 0) process.stderr.write('.')
  if (halt) break
}
console.error()

const { root, additions } = await batch.commit()
await new ReadableStream({
  pull (controller) {
    const block = additions.shift()
    if (!block) return controller.close()
    controller.enqueue(block)
  }
})
.pipeThrough(new CARWriterStream([root]))
.pipeTo(Writable.toWeb(process.stdout))

console.error(`✅ successfully created pail with root ${root}`)
if (halt) console.error('⚠️ process was interrupted, pail does NOT contain complete listing')
