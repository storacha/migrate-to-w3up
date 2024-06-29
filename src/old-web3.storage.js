import { Web3Storage } from "web3.storage"
import { W32023Upload } from "../src/w32023.js";

const API = 'https://api.web3.storage'

/**
 * get a stream of w32023 uploads via
 * interactive prompts using inquirer
 * + old web3.storage client library 
 * @param {object} options - options
 * @param {string} [options.api] - optional API endpoint override
 * @param {string} options.token
 * @returns {AsyncIterable<W32023Upload> & { length: Promise<number> }} uploads
 */
export function getUploads({
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
  const oldW3 = new Web3Storage({ token, endpoint })
  const uploads = (async function* () {
    for await (const u of oldW3.list()) {
      if (u) {
        yield new W32023Upload(u)
      }
    }
  }())
  // get count
  const userUploadsResponse = fetch(`${api}/user/uploads`, {
    headers: { authorization: `Bearer ${token}` },
  })
  const count = userUploadsResponse.then(r => parseInt(r.headers.get('count'), 10)).then(c => isNaN(c) ? undefined : c)
  return Object.assign(uploads, { length: count })
}