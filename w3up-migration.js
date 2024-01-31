/**
 * @template Upload
 * 
 * a single block that has been migrated to w3up.
 * i.e. it has a store/add receipt.
 * if the receipt instructed the client to send car bytes, that already happened too
 */
export class MigratedUploadOnePart {
  /** @type {Upload} */
  upload
  /** @type {string} */
  part
  /**
   * @type {{
   *   receipt: import('@ucanto/interface').Receipt<import("@web3-storage/access").StoreAddSuccess>
   * }}
   */
  add
  /**
   * @type {undefined|{
   *   response: Response
   * }}
   */
  copy
}

/**
 * @template Upload
 * 
 * a single upload with all blocks migrated to w3up.
 */
export class MigratedUploadAllParts {
  /** @type {Upload} */
  upload
  /**
   * map of part CID to migrated part block
   * @type {Map<string, MigratedUploadOnePart<Upload>>}
   */
  parts
}

/**
 * @template Upload
 * 
 * a single upload with all blocks migrated to w3up
 * AND an upload/add receipt
 */
export class MigratedUpload {
  /** @type {Upload} */
  upload

  /**
   * @type {{
   *  receipt: import('@ucanto/interface').Receipt<import("@web3-storage/access").UploadAddSuccess>
   * }}
   */
  add

  /**
   * map of part CID to migrated part block
   * @type {Map<string, MigratedUploadOnePart<Upload>>}
   */
  parts
}
