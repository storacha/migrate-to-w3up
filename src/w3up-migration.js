import readNDJSONStream from 'ndjson-readablestream'
import { ReadableStream } from 'node:stream/web'

/**
 * @template {{ cid: string }} Upload
 * 
 * a single block that has been migrated to w3up.
 * i.e. it has a store/add receipt.
 * if the receipt instructed the client to send car bytes, that already happened too
 */
export class MigratedUploadPart {
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
  /** @type {string} */
  part
  /** @type {Upload} */
  upload

  toJSON() {
    return {
      type: 'MigratedUploadPart',
      copy: this.copy,
      part: this.part,
      add: {
        receipt: receiptToJson(this.add.receipt),
      },
      upload: {
        cid: this.upload.cid,
      }
    }
  }
}

/**
 * @template {{ cid: string }} Upload
 * 
 * a single upload with all blocks migrated to w3up.
 */
export class MigratedUploadParts {
  /**
   * map of part CID to migrated part block
   * @type {Map<string, MigratedUploadPart<Upload>>}
   */
  parts
  /** @type {Upload} */
  upload
}

/**
 * @template {{ cid: string }} Upload
 * 
 * a single upload with all blocks migrated to w3up
 * AND an upload/add receipt
 */
export class UploadMigrationSuccess {
  /**
   * @type {{
   *  receipt: import('@ucanto/interface').Receipt<import("@web3-storage/access").UploadAddSuccess>
   * }}
   */
  add

  /**
   * map of part CID to migrated part block
   * @type {Map<string, MigratedUploadPart<Upload>>}
   */
  parts

  /** @type {Upload} */
  upload

  toJSON() {
    return {
      type: 'UploadMigrationSuccess',
      parts: Object.fromEntries([...this.parts.entries()].map(([partCid, migratedPart]) => {
        return [partCid, migratedPart.toJSON()]
      })),
      add: {
        receipt: receiptToJson(this.add.receipt),
      },
      upload: this.upload,
    }
  }
}

/**
 * @template {{ cid: string }} Upload
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

  toJSON() {
    return {
      part: this.part,
      upload: { cid: this.upload.cid },
      // @ts-expect-error 'toJSON' may be there in practice
      cause: ('toJSON' in this.cause && typeof this.cause === 'function') ? this.cause.toJSON() : this.cause,
    }
  }
}

/**
 * @template {{ cid: string }} Upload
 * @template {Error} [E=Error]
 * 
 * a single upload that could not be migrated due to an Error
 */
export class UploadMigrationFailure {
  /** @type {E} */
  cause

  /**
   * map of part CID to migrated part block (or failure to migrate part)
   * @type {Map<string, MigratedUploadPart<Upload>|UploadPartMigrationFailure<Upload>>}
   */
  parts

  /** @type {Upload} */
  upload

  toJSON() {
    return {
      type: 'UploadMigrationFailure',
      cause: this.cause,
      upload: this.upload,
      parts: Object.fromEntries([...this.parts.entries()].map(([partCid, partMigration]) => {
        return [partCid, 'toJSON' in partMigration ? partMigration.toJSON() : partMigration]
      }))
    }
  }
}

export class UnexpectedFailureReceipt extends Error {
  /**
   * @param {string} message - error message
   * @param {import('@ucanto/interface').Receipt} receipt - receipt with failure
   * @param {object} [options] - options
   * @param {unknown} [options.cause] - cause of error
   */
  constructor(message, receipt, options={}) {
    super(message, options)
    this.name = 'UnexpectedFailureReceipt'
    this.receipt = receipt
  }
  toJSON() {
    return {
      message: this.message,
      name: this.name,
      cause: this.cause,
      receipt: receiptToJson(this.receipt),
    }
  }
}

/**
 * @param {import('@ucanto/interface').Receipt} r - receipt
 */
export function receiptToJson(r) {
  return {
    type: 'Receipt',
    ran: 'root' in r.ran ? invocationToJson(r.ran) : r.ran,
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

/**
 * @param {ReadableStream} readable - readable stream of ndjson migration events
 * @returns {ReadableStream} - uploads extracted from UploadMigrationFailures in readable
 */
export function readUploadsFromUploadMigrationFailuresNdjson(readable) {
  return new ReadableStream({
    async start(controller) {
      for await (const event of readNDJSONStream(readable)) {
        if (event?.type === 'UploadMigrationFailure') {
          controller.enqueue(JSON.stringify(event.upload) + '\n')
        }
      }
      controller.close()
    }
  })
}
