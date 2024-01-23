/**
 * @fileoverview tools related to the upload JSON format served by old.web3.storage
 */

export const exampleUpload1 = {"_id":"315318962269342672","type":"Car","name":"Upload at 2024-01-22T19:10:44.048Z","created":"2024-01-22T19:10:44.048+00:00","updated":"2024-01-22T19:10:44.048+00:00","cid":"bafybeieevwnu57cbcp5u6jsy6wxpj2waq5gfq5gc4spss4skpzk34vvxyy","dagSize":8848144,"pins":[{"status":"Pinned","updated":"2024-01-22T19:10:44.048+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaierakuersmo7wndedhwk43e5xwcpzwenuda3dhpcsvkfibewg5gxl7oa"],"deals":[]}

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
   * @param {string} input 
   */
  static from (input) {
    const upload = typeof input === 'string' ? JSON.parse(input): input
    return new W32023Upload(upload)
  }
  /**
   * @param {W32023Upload} upload 
   */
  constructor(upload) {
    this.#upload = upload
    Object.assign(this, upload)
  }
}
