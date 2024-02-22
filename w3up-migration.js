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

/**
 * @template Upload
 * @template {Error} [E=Error]
 * 
 * a single upload *car part* that could not be migrated due to an Error
 */
export class UploadPartMigrationFailure {
  /** @type {string} */
  part

  /** @type {Upload} */
  upload

  /** @type {E} */
  cause
}

/**
 * @template Upload
 * @template {Error} [E=Error]
 * 
 * a single upload that could not be migrated due to an Error
 */
export class MigrateUploadFailure {
  /** @type {Upload} */
  upload

  /** @type {E} */
  cause

  /**
   * map of part CID to migrated part block (or failure to migrate part)
   * @type {Map<string, MigratedUploadOnePart<Upload>|UploadPartMigrationFailure<Upload>>}
   */
  parts
}

/**
 * @param {import('@ucanto/interface').Receipt} r - receipt
 */
export function receiptToJson(r) {
  return {
    type: 'Receipt',
    ran: 'root' in r.ran ? invocationToJson(r.ran) : r,
    out: {
      ok: r.out.ok,
      error: r.out.error,
    },
    fx: r.fx,
    meta: r.meta,
    issuer: r.issuer,
    signature: r.signature,
  }
}

/**
 * @param {import('@ucanto/interface').Invocation} i - invocation
 */
function invocationToJson(i) {
  return {
    type: 'Invocation',
    cid: i.cid,
    issuer: i.issuer.did(),
    audience: i.audience.did(),
    capabilities: i.capabilities,
    signature: i.signature,
    expiration: i.expiration,
  }
}
