export const API = 'https://api.nft.storage'

export function getClient ({
  api = API,
  token
}) {
  if (!token) {
    console.log('! run `nft token` to set an API token to use')
    process.exit(-1)
  }
  const endpoint = new URL(api)
  if (api !== API) {
    // note if we're using something other than prod.
    console.log(`using ${endpoint.hostname}`)
  }
  return new NftStorage({ token, endpoint })
}

/**
 * @typedef {object} Service
 * @property {URL} [endpoint]
 * @property {string} token
 */

class NftStorage {
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
          ...NftStorage.headers(token)
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
