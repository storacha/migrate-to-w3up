/**
 * @file tools related to the upload JSON format served by old.web3.storage
 */

import readNDJSONStream from 'ndjson-readablestream';
import stream from 'node:stream'

export const exampleUpload1 = {"_id":"315318962269342672","type":"Car","name":"Upload at 2024-01-22T19:10:44.048Z","created":"2024-01-22T19:10:44.048+00:00","updated":"2024-01-22T19:10:44.048+00:00","cid":"bafybeieevwnu57cbcp5u6jsy6wxpj2waq5gfq5gc4spss4skpzk34vvxyy","dagSize":8848144,"pins":[{"status":"Pinned","updated":"2024-01-22T19:10:44.048+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaierakuersmo7wndedhwk43e5xwcpzwenuda3dhpcsvkfibewg5gxl7oa"],"deals":[]}

export class W32023UploadsFromNdjson {
  /** @type {AsyncIterable<Uint8Array>} */
  #uploadsNdjson
  /**
   * @param {AsyncIterable<Uint8Array>} uploadsNdjson - uploads as w32023 objects in ndjson
   */
  constructor(uploadsNdjson) {
    this.#uploadsNdjson = uploadsNdjson
  }
  async *[Symbol.asyncIterator]() {
    const iterateUploadObjects = () => readNDJSONStream(stream.Readable.toWeb(stream.Readable.from(this.#uploadsNdjson)))
    for await (const object of iterateUploadObjects()) {
      yield W32023Upload.from(object)
    }
  }
}

export class W32023Upload {
  /** @type {string} */
  _id
  /** @type {'Car'} */
  type
  /** @type {string} */
  name
  /** @type {string} */
  created
  /** @type {string} */
  updated
  /** @type {string} */
  cid
  /** @type {number} */
  dagSize
  /**
   * @type {Array<{
   *   status: string
   *   updated: string
   *   peerId: string
   *   peerName: string
   *   region: null
   * }>}
   */
  pins
  /**
   * array of CID strings
   * @type {Array<string>}
   */
  parts
  /** @type {unknown[]} */
  deals

  /** @type {object} */
  #upload;

  /**
   * @param {string|object} input - becomes a W32023Upload
   */
  static from (input) {
    const upload = typeof input === 'string' ? JSON.parse(input): input
    return new W32023Upload(upload)
  }
  /**
   * @param {W32023Upload} upload - becomes a W32023Upload
   */
  constructor(upload) {
    this.#upload = upload
    Object.assign(this, upload)
  }
}

export class W32023UploadSummary {
  /** @type {W32023Upload} */
  #upload;
  constructor(upload) {
    this.#upload = upload
  }
  toJSON() {
    return {
      _id: this.#upload._id,
      cid: this.#upload.cid,
      name: this.#upload.name,
      parts: this.#upload.parts,
      created: this.#upload.created,
      updated: this.#upload.updated,
    }
  }
}
