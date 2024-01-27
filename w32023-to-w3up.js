import { W32023Upload } from "./w32023.js";
import * as Link from 'multiformats/link'

/**
 * @param {W32023Upload} upload - upload to convert to store/add invocations
 */
export async function * toStoreAdd(upload) {
  for (const part of upload.parts) {
    const partUrl = `https://w3s.link/ipfs/${part}`;
    const carHeadResponse = await fetch(partUrl, { method: 'HEAD' })
    const carSizeString = carHeadResponse.headers.get('content-length')
    const carSize = carSizeString && parseInt(carSizeString, 10)
    if ( ! carSize) {
      throw new Error(`unable to determine carSize for ${partUrl}`)
    }
    const add = {
      nb: {
        size: carSize,
        link: Link.parse(part),
      },
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
