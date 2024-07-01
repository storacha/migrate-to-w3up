import { W32023Upload } from "../src/w32023.js";

export const API = 'https://api.nft.storage'

/**
 * get a stream of w32023 uploads from nft.storage
 * @param {object} options - options
 * @param {string} [options.api] - optional API endpoint override
 * @param {string} options.token
 * @returns {AsyncIterable<W32023Upload> & { length: Promise<number> }} uploads
 */
export function getUploads ({
  api = API,
  token
}) {
  if (!token) {
    throw new Error('! run `nft token` to set an API token to use')
  }
  const endpoint = new URL(api)
  if (api !== API) {
    // note if we're using something other than prod.
    console.info(`using ${endpoint.hostname}`)
  }
  const classicNftStorage = new NFTStorage({ token, endpoint })

  const uploads = (async function* () {
    for await (const u of classicNftStorage.list()) {
      if (u) {
        yield new W32023Upload(u)
      }
    }
  }())

  return Object.assign(uploads, { length: classicNftStorage.count() })
}

/**
 * @typedef {object} Service
 * @property {URL} [endpoint]
 * @property {string} token
 */

class NFTStorage {
  constructor ({ token, endpoint }) {
    this.token = token
    this.endpoint = endpoint
  }

  /**
   * @hidden
   * @param {string} token
   * @returns {Record<string, string>}
   */
  static headers (token) {
    if (!token) throw new Error('missing token')
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    }
  }

  count() {
    const res = fetch(this.endpoint.toString(), {
      method: 'GET',
      headers: {
        ...NFTStorage.headers(this.token)
      },
    })
    return res.then(r => parseInt(r.headers.get('count'), 10)).then(c => isNaN(c) ? undefined : c)
  }

  /**
   * @param {{before?: string, size?: number}} opts
   */
  async* list (opts = {}) {
    const service = {
      token: this.token,
      endpoint: this.endpoint
    }
    /**
     * @param {Service} service
     * @param {{before: string, size: number, signal: any}} opts
     * @returns {Promise<Response>}
     */
    async function listPage ({ endpoint, token }, { before, size }) {
      const params = new URLSearchParams()
      // Only add params if defined
      if (before) {
        params.append('before', before)
      }
      if (size) {
        params.append('limit', String(size))
      }
      const url = new URL(`?${params}`, endpoint)
      return fetch(url.toString(), {
        method: 'GET',
        headers: {
          ...NFTStorage.headers(token)
        },
      })
    }

    for await (const res of paginator(listPage, service, opts)) {
      for (const upload of res.value) {
        yield upload
      }
    }
  }
}

/**
 * Follow before with last item, to fetch all the things.
 *
 * @param {(service: Service, opts: any) => Promise<Response>} fn
 * @param {Service} service
 * @param {{}} opts
 */
async function * paginator (fn, service, opts) {
  let res = await fn(service, opts)
  if (!res.ok) {
    if (res.status === 429) {
      throw new Error('rate limited')
    }

    const errorMessage = await res.json()
    throw new Error(`${res.status} ${res.statusText} ${errorMessage ? '- ' + errorMessage.message : ''}`)
  }
  let body = await res.json()
  yield body

  // Iterate through next pages
  while (body && body.value.length) {
    // Get before timestamp with less 1ms
    const before = (new Date((new Date(body.value[body.value.length-1].created)).getTime() - 1)).toISOString()
    res = await fn(service, {
      ...opts,
      before
    })

    body = await res.json()

    yield body
  }
}
