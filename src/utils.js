import { fromString } from 'uint8arrays'
import * as Digest from 'multiformats/hashes/digest'
import * as Link from 'multiformats/link'

/**
 * keep track of a map of content id string to an array of 'promise resolvers'.
 * This can be useful e.g. to represent as a promise resolver potentially
 * many open connections for the same cid.
 * And once you have resolved the cid, iterate over the resolvers for the cid and resolve.reject them.
 */
export class MapCidToPromiseResolvers {
 constructor() {
   /**
    * @type {Map<string, Array<{
    *   resolve:(v: any) => void
    *   reject: (err?: Error) => void
    * }>>}
    */
   this.cidToResolvers = new Map
 }
 get size() {
   let count = 0;
   for (const [, value] of this.cidToResolvers.entries()) {
     count += value.length
   }
   return count
 }
 *[Symbol.iterator]() {
   for (const hangs of this.cidToResolvers.values()) {
     yield * hangs
   }
 }
 delete(cid) {
   this.cidToResolvers.delete(cid)
 }
 push(cid, { resolve, reject }) {
   const cleanup = () => {
     this.delete(cid)
   }
   const hangs = this.cidToResolvers.get(cid) ?? []
   const hang = {
     resolve(v) {
       cleanup()
       resolve(v)
     },
     reject(e) {
       cleanup()
       reject(e)
     },
   }
   this.cidToResolvers.set(cid, [...hangs, hang])
 }
}

// multicodec codec for CAR bytes
const CAR_CODE = 0x0202

/**
 * Attempts to extract a CAR CID from a bucket key.
 * @param {string} key - string to parse to a CAR CID. e.g. a carpark bucket key basename
 */
export const stringToCarCid = key => {
  let errParseAsCid
  try {
    // recent buckets encode CAR CID in filename
    const cid = Link.parse(key).toV1()
    return cid
  } catch (err) {
    errParseAsCid = err
  }
  // older buckets base32 encode a CAR multihash <base32(car-multihash)>.car
  // try to parse as base32
  let errParseAsBase32
  try {
    const digestBytes = fromString(key, 'base32')
    const digest = Digest.decode(digestBytes)
    return Link.create(CAR_CODE, digest)
  } catch (error) {
    errParseAsBase32 = error
  }

  if (errParseAsCid || errParseAsBase32) {
    throw new Error(`unable to parse string to CAR CID`, {
      cause: {
        errParseAsCid,
        errParseAsBase32,
      }
    })
  }
}
