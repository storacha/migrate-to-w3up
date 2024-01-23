import { W32023Upload } from "./w32023.js";
import * as Link from 'multiformats/link'

/**
 * @param {W32023Upload} upload
 */
export function * toStoreAdd(upload) {
  for (const part of upload.parts) {
    /** @type {import("@web3-storage/w3up-client/types").StoreAdd['nb']} */
    const add = {
      size: upload.dagSize,
      link: Link.parse(part),
    }
    yield add
  }
}

export default {
  toStoreAdd,
}
