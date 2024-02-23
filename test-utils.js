import { IncomingMessage } from 'http'

/**
 * @param {object} options - options
 * @param {(request: IncomingMessage) => string|undefined} [options.cid] - given request, return a relevant CAR CID to find
 * @param {(request: IncomingMessage) => Record<string,string>} options.headers - given a request, return map that should be used for http response headers, e.g. to add a content-length header like w3s.link does.
 * @param {(request: IncomingMessage) => Promise<any>} [options.waitToRespond] - if provided, this can return a promise that can defer responding
 * @returns {import('http').RequestListener} request listener that mocks w3s.link
 */
export function createCarFinder(options) {
  return (req, res) => {
    (options.waitToRespond?.(req) ?? Promise.resolve()).then(() => {
      const getCid = options.cid ?? function (req) {
        const lastPathSegmentMatch = req.url.match(/\/([^/]+)$/)
        const cid = lastPathSegmentMatch && lastPathSegmentMatch[1]
        return cid
      }
      const cid = getCid(req)
      if (!cid) {
        res.writeHead(404)
        res.end('cant determine cid');
        return;
      }
      const headers = options.headers(req) ?? {}
      if (!Object.keys(headers).find(h => h.match(/content-length/i))) {
        throw new Error('carFinder response headers must include content-length')
      }
      res.writeHead(200, {
        ...headers
      })
      res.end()
    })
  }
}

/**
 * @param {import('http').Server} server - server that should be listening on the returned url
 */
export function locate(server) {
  const address = server.address()
  if (typeof address === 'string') throw new Error(`unexpected address string`)
  const { port } = address
  const url = new URL(`http://localhost:${port}/`)
  return { url }
}
