import { W32023Upload } from "./w32023.js";
import * as Link from 'multiformats/link'

/**
 * @param {W32023Upload} upload
 */
export async function * toStoreAdd(upload) {
  for (const part of upload.parts) {
    const partUrl = `https://w3s.link/ipfs/${part}`;
    console.debug('fetching HEAD', partUrl)
    const carHeadResponse = await fetch(partUrl, { method: 'HEAD' })
    console.debug('fetched HEAD', partUrl, { status: carHeadResponse.status })
    const carSizeString = carHeadResponse.headers.get('content-length')
    const carSize = carSizeString && parseInt(carSizeString, 10)
    if ( ! carSize) {
      throw new Error(`unable to determine carSize for ${partUrl}`)
    }
    const add = {
      size: carSize,
      link: Link.parse(part),
      part,
      partUrl,
      get headResponse() { return carHeadResponse },
    }
    yield add
  }
}

export default {
  toStoreAdd,
}
