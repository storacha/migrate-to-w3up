import { CID } from 'multiformats/cid'
import { identity } from 'multiformats/hashes/identity'
import { CarReader, CarWriter } from '@ipld/car'
import { importDAG } from '@ucanto/core/delegation'
import * as ucanto from '@ucanto/core'

/**
 * @param {string} proof - proof encoded as CID
 */
export async function parseW3Proof(proof) {
  let cid
  try {
    cid = CID.parse(proof)
  } catch (/** @type {any} */ err) {
    if (err?.message?.includes('Unexpected end of data')) {
      console.error(`Error: failed to read proof. The string has been truncated.`)
    }
    throw err
  }

  if (cid.multihash.code !== identity.code) {
    console.error(`Error: failed to read proof. Must be identity CID. Fetching of remote proof CARs not supported by this command yet`)
    process.exit(1)
  }
  const delegation = await readProofFromBytes(cid.multihash.digest)
  return delegation
}

/**
 * @param {Uint8Array} bytes Path to the proof file.
 */
export async function readProofFromBytes(bytes) {
  const blocks = []
  try {
    const reader = await CarReader.fromBytes(bytes)
    for await (const block of reader.blocks()) {
      blocks.push(block)
    }
  } catch (/** @type {any} */ err) {
    console.error(`Error: failed to parse proof: ${err.message}`)
    throw err
  }
  try {
    return importDAG(blocks)
  } catch (/** @type {any} */ err) {
    console.error(`Error: failed to import proof: ${err.message}`)
    throw err
  }
}

/**
 * @param {import('@ucanto/interface').Delegation} delegation - delegation to encode
 */
export async function encodeDelegationAsCid(delegation) {
  const { writer, out } = CarWriter.create()
  const carChunks = []
  await Promise.all([
    // write delegation blocks
    (async () => {
      for (const block of delegation.export()) {
        await writer.put(block)
      }
      await writer.close()
    })(),
    // read out
    (async () => {
      for await (const chunk of out) { carChunks.push(chunk) }
    })()
  ])
  // now get car chunks
  const car = new Blob(carChunks)
  const bytes = new Uint8Array(await car.arrayBuffer())
  const cid = CID.createV1(ucanto.CAR.code, identity.digest(bytes))
  return cid
}
