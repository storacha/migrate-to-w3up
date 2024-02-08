import { W32023Upload } from "./w32023.js";
import * as Link from 'multiformats/link'
import { Store, Upload } from '@web3-storage/capabilities'
import { DID } from "@ucanto/validator"
import { Parallel } from 'parallel-transform-web'
import { MigratedUpload, MigratedUploadAllParts, MigratedUploadOnePart } from "./w3up-migration.js";

/**
 * migrate from w32023 to w3up.
 * returns an AsyncIterable of migration events as they occur.
 * @param {object} options - options
 * @param {import("@ucanto/client").SignerKey} options.issuer - principal that will issue w3up invocations
 * @param {Authorization} [options.authorization] - authorization sent with w3up invocations
 * @param {import("@ucanto/client").ConnectionView} options.w3up - connection to w3up on which invocations will be sent
 * @param {URL} options.destination - e.g. w3up space DID to which source uploads will be migrated
 * @param {AbortSignal} [options.signal] - for cancelling the migration
 * @param {ReadableStream<W32023Upload>} options.source - uploads that will be migrated from w32023 json format to w3up
 * @param {number} [options.concurrency] - max concurrency for any phase of pipeline
 * @param {(part: string, options?: { signal?: AbortSignal }) => Promise<Response>} options.fetchPart - given a part CID, return the fetched response
 * @yields {MigratedUpload<W32023Upload>}
 */
export async function* migrate(options) {
  const {
    issuer,
    authorization,
    w3up,
    destination,
    signal,
    source,
    concurrency = 1,
    fetchPart,
  } = options;
  if (concurrency < 1) {
    throw new Error(`concurrency must be at least 1`)
  }
  const results = source
    .pipeThrough(new TransformStream(new UploadToFetchableUploadPart({ fetchPart })))
    .pipeThrough(
      new Parallel(concurrency, (part) => migratePart({
        ...options,
        part,
      }))
    )
    .pipeThrough(new TransformStream(new CollectMigratedUploadParts))
    .pipeThrough(new TransformStream(new InvokeUploadAddForMigratedParts({ w3up, issuer, destination, authorization, signal })))
  const reader = results.getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) {
      return;
    }
    yield value
  }
}

/**
 * proof of Authorization required to write to w3up
 * @typedef {import('@ucanto/interface').Delegation[]} Authorization
 */

/**
 * a single part of an upload, with the part fetchable
 * @typedef FetchableUploadPart
 * @property {string} part - cid of car part
 * @property {W32023Upload} upload - upload that has part in .parts
 * @property {(options?:{signal?:AbortSignal}) => Promise<Response>} fetch - fetch the car bytes
 */

/**
 * a single part of an upload, with the part fetched
 * @typedef UploadPartWithResponse
 * @property {string} part - cid of car part
 * @property {W32023Upload} upload - upload that has part in .parts
 * @property {Response} response - result of fetching part
 */

/**
 * migrate one (car) part of an upload to w3up.
 * this is called concurrently by `migrate` below.
 * @param {object} options - options
 * @param {import("@ucanto/client").SignerKey} options.issuer - principal that will issue w3up invocations
 * @param {Authorization} [options.authorization] - authorization sent with w3up invocations
 * @param {import("@ucanto/client").ConnectionView} options.w3up - connection to w3up on which invocations will be sent
 * @param {FetchableUploadPart} options.part - upload to transform
 * @param {URL} options.destination - e.g. w3up space DID to which source uploads will be migrated
 * @param {AbortSignal} [options.signal] - for cancelling the migration
 */
async function migratePart({ part, signal, issuer, authorization, destination, w3up }) {
  signal?.throwIfAborted()
  const space = DID.match({ method: 'key' }).from(destination.toString())
  const partFetchResponse = await part.fetch({ signal })
  const addNb = carPartToStoreAddNb({ ...part, response: partFetchResponse })
  const invocation = Store.add.invoke({
    issuer,
    audience: w3up.id,
    proofs: authorization,
    with: space,
    nb: addNb,
  })
  const receipt = await invocation.execute(w3up)
  const storeAddSuccess = receipt.out.ok
  const copyResponse = storeAddSuccess && await uploadBlockForStoreAddSuccess(
    // @ts-expect-error no svc type
    receipt.out.ok,
    partFetchResponse,
  )
  /**
   * @type {MigratedUploadOnePart<W32023Upload>}
   */
  const output = {
    ...part,
    add: {
      // @ts-expect-error - receipt has no service type to guarantee StoreAddSuccess
      receipt,
    },
    copy: copyResponse && {
      response: copyResponse,
    },
  }
  return output
}

/**
 * use UploadPartWithResponse to get argument to store/add invocation.
 * store/add requires .nb.size, which comes from the response 'content-length' header.
 * @param {Pick<UploadPartWithResponse, 'part'|'response'>} options - options
 */
export function carPartToStoreAddNb(options) {
  const carSizeString = options.response.headers.get('content-length')
  const carSize = carSizeString && parseInt(carSizeString, 10)
  if (!carSize) {
    throw new Error(`unable to determine carSize for response to ${options.response.url}`)
  }
  const link = Link.parse(options.part)
  /** @type {import("@web3-storage/access").StoreAdd['nb']} */
  const addNb = {
    link,
    size: carSize
  }
  return addNb
}

/**
 * transform each upload part with fetched response,
 * collect all parts for an upload, then yield { upload, parts }.
 * If you want low memory usage, be sure to order inputs by upload.
 * @param {MigratedUploadOnePart<W32023Upload>} migratedPart - input to transform
 * @param {object} options - options
 * @param {AbortSignal} [options.signal] - for cancelling the migration
 * @param {Map<string, Map<string, MigratedUploadOnePart<W32023Upload>>>} [options.uploadCidToParts] - Map<upload.cid, Map<part.cid, { response }>> - where to store state while waiting for all parts of an upload
 * @yields {MigratedUploadAllParts<W32023Upload>} upload with all parts
 */
const collectMigratedParts = async function* (
  migratedPart,
  {
    signal,
    uploadCidToParts = new Map
  } = {}
) {
  signal?.throwIfAborted()
  const {
    upload,
  } = migratedPart
  if (!uploadCidToParts.has(upload.cid)) {
    uploadCidToParts.set(upload.cid, new Map)
  }
  const partsForUpload = uploadCidToParts.get(upload.cid)
  if (!partsForUpload) throw new Error(`unexpected falsy parts`)
  partsForUpload.set(migratedPart.part, migratedPart)

  let missingPart = false
  const partsReceived = uploadCidToParts.get(upload.cid)
  for (const part of upload.parts) {
    if (!partsReceived.has(part)) {
      missingPart = true
      break;
    }
  }
  const collectedAllParts = !missingPart

  if (collectedAllParts) {
    // no need to keep this memory around
    uploadCidToParts.delete(upload.cid)
    /** @type {MigratedUploadAllParts<W32023Upload>} */
    const allparts = {
      upload,
      parts: partsForUpload,
    }
    yield allparts
  } else {
    // console.debug('still waiting for car parts', {
    //   'partsForUpload.size': partsForUpload.size,
    //   'upload.parts.length': upload.parts.length,
    // })
  }
}

/**
 * given stream of migrated upload parts (preferably with parts from same upload in sequence),
 * transform to stream of migrated uploads with all parts.
 * i.e. 'group by upload'
 * @implements {Transformer<
 *   MigratedUploadOnePart<W32023Upload>,
 *   MigratedUploadAllParts<W32023Upload>
 * >}
 */
class CollectMigratedUploadParts {
  static collectMigratedParts = collectMigratedParts
  /**
   * @param {AbortSignal} [signal] - emits event when this transformer should abort
   */
  constructor(signal) {
    this.signal = signal
  }
  /**
   * @param {MigratedUploadOnePart<W32023Upload>} input - input for each part in upload.parts
   * @param {TransformStreamDefaultController} controller - enqueue output here
   */
  async transform(input, controller) {
    try {
      for await (const output of CollectMigratedUploadParts.collectMigratedParts(input, this)) {
        controller.enqueue(output)
      }
    } catch (error) {
      controller.enqueue(error)
      throw error
    }
  }
}

/**
 * transform each upload into many upload.parts + add method for part to be fetched
 * (e.g. via http) to get the referent of the part link, i.e. the part car bytes
 * @param {W32023Upload} upload - upload with parts
 * @param {(part: string, options?: { signal?: AbortSignal }) => Promise<Response>} fetchPart - given a part CID, fetch it and return a Response
 * @yields {FetchableUploadPart} for each part in upload.parts
 */
const transformUploadToFetchableUploadPart = async function* (upload, fetchPart) {
  for (const part of upload.parts) {
    /** @type {FetchableUploadPart} */
    const withResponse = {
      upload,
      part,
      fetch({ signal }) {
        return fetchPart(part, { signal })
      },
    }
    yield withResponse
  }
}

/**
 * transform each upload into many upload.parts + add method for part to be fetched
 * (e.g. via http) to get the referent of the part link, i.e. the part car bytes.
 * @implements {Transformer<
 *   W32023Upload,
 *   FetchableUploadPart
 * >}
 */
class UploadToFetchableUploadPart {
  static transformUploadToFetchableUploadPart = transformUploadToFetchableUploadPart
  /**
   * @param {object} options - options
   * @param {(part: string, options?: { signal?: AbortSignal }) => Promise<Response>} options.fetchPart - given a part CID, return the fetched response
   */
  constructor({ fetchPart }) {
    this.fetchPart = fetchPart
  }
  /**
   * @param {W32023Upload} upload - upload to transform into one output per upload.part
   * @param {TransformStreamDefaultController} controller - enqueue output her
   */
  async transform(upload, controller) {
    for await (const out of UploadToFetchableUploadPart.transformUploadToFetchableUploadPart(upload, this.fetchPart)) {
      controller.enqueue(out)
    }
  }
}

/**
 * when store/add succeeds, the result instructs the client how to ensure w3up has the block bytes.
 * This function handles that instruction,
 * doing nothing if the StoreAddSuccess indicates w3up already has the block,
 * and piping car bytes to the instructed destination if not.
 * @param {import("@web3-storage/access").StoreAddSuccess} storeAddSuccess - successful store/add result
 * @param {ReadableStream<Uint8Array>} car - car bytes
 * @param {object} [options] - options
 * @param {AbortSignal} [options.signal] - emits when this should abort
 */
async function uploadBlockForStoreAddSuccess(
  storeAddSuccess,
  car,
  options = {}
) {
  switch (storeAddSuccess.status) {
    case "done":
      // no work needed
      return
    case "upload":
      break;
    default:
      // @ts-expect-error storeAddSuccess could be never type, but in practice something else
      throw new Error(`unexpected store/add success status: "${storeAddSuccess.status}"`)
  }
  // need to do upload
  const sendCarRequest = new Request(
    storeAddSuccess.url,
    {
      method: 'PUT',
      mode: 'cors',
      headers: storeAddSuccess.headers,
      body: car,
      redirect: 'follow',
      // @ts-expect-error not in types, but required for the body to work
      duplex: 'half'
    }
  )
  const sendToPresignedResponse = await fetch(sendCarRequest, { signal: options.signal })
  // ensure was 2xx, otherwise throw because something unusual happened
  if (!(200 <= sendToPresignedResponse.status && sendToPresignedResponse.status < 300)) {
    console.warn('unsuccessful sendToPresignedResponse', sendToPresignedResponse)
    throw Object.assign(
      new Error(`error sending car bytes to url from store/add response`), {
      response: sendToPresignedResponse,
    }
    )
  }
  return sendToPresignedResponse
}

/**
 * given info about an upload with all parts migrated to w3up,
 * invoke upload/add with the part links to complete migrating the upload itself.
 * @param {MigratedUploadAllParts<W32023Upload>} upload - upload with all parts migrated to destination
 * @param {object} options - options
 * @param {import("@ucanto/client").ConnectionView} options.w3up - connection to w3up on which invocations will be sent
 * @param {import("@ucanto/client").SignerKey} options.issuer - principal that will issue w3up invocations
 * @param {Authorization} options.authorization - authorization sent with w3up invocations
 * @param {URL} options.destination - e.g. w3up space DID to which source uploads will be migrated
 * @param {AbortSignal} [options.signal] - for cancelling the migration
 */
async function transformInvokeUploadAddForMigratedUploadParts({ upload, parts }, { issuer, authorization, destination, w3up, signal }) {
  const shards = upload.parts.map(c => Link.parse(c).toV1())
  const root = Link.parse(upload.cid)
  const space = DID.match({ method: 'key' }).from(destination.toString())
  const uploadAddReceipt = await Upload.add.invoke({
    issuer,
    audience: w3up.id,
    proofs: authorization,
    with: space,
    nb: {
      root,
      // @ts-expect-error tolerate any link vs car link
      shards,
    },
  }).execute(
    w3up,
  )
  if (!uploadAddReceipt.out.ok) {
    console.log('uploadAddReceipt.out', uploadAddReceipt.out)
    throw new Error(`upload/add failure`)
  }
  const receipt = /** @type {import('@ucanto/interface').Receipt<import("@web3-storage/access").UploadAddSuccess>} */ (
    uploadAddReceipt
  )
  return { upload, parts, add: { receipt } }
}

/**
 * transform stream of info about uploads with all parts migrated to w3up,
 * into stream of info about uploads migrated via successful upload/add invocation linking to parts.
 * @implements {Transformer<
 *   MigratedUploadAllParts<W32023Upload>,
 *   MigratedUpload<W32023Upload>
 * >}
 */
class InvokeUploadAddForMigratedParts {
  static transform = transformInvokeUploadAddForMigratedUploadParts
  /**
   * @param {object} options - options
   * @param {import("@ucanto/client").ConnectionView} options.w3up - connection to w3up on which invocations will be sent
   * @param {import("@ucanto/client").SignerKey} options.issuer - principal that will issue w3up invocations
   * @param {Authorization} [options.authorization] - authorization sent with w3up invocations
   * @param {URL} options.destination - e.g. w3up space DID to which source uploads will be migrated
   * @param {AbortSignal} [options.signal] - for cancelling the migration
   */
  constructor({ w3up, issuer, authorization, destination, signal }) {
    /**
     * @param {MigratedUploadAllParts<W32023Upload>} uploadedParts - upload to transform into one output per upload.part
     * @param {TransformStreamDefaultController<MigratedUpload<W32023Upload>>} controller - enqueue output her
     */
    this.transform = async function transform(uploadedParts, controller) {
      controller.enqueue(await InvokeUploadAddForMigratedParts.transform(uploadedParts, { w3up, issuer, authorization, destination, signal }))
    }
  }
}
