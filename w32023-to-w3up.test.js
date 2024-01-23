import { describe, test } from 'node:test'
import { W32023Upload, W32023UploadsFromNdjson } from './w32023.js'
import fromW32023ToW3up from './w32023-to-w3up.js'
import assert from 'assert'
import * as Link from 'multiformats/link'
import * as nodeHttp from 'node:http'
import * as CAR from "@ucanto/transport/car"
import * as HTTP from "@ucanto/transport/http"
import * as Client from '@ucanto/client'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as Server from "@ucanto/server"
import { Store } from '@web3-storage/capabilities'
import { Readable } from 'stream'
import { ReadableStream } from 'stream/web'

/** example uploads from `w3 list --json` */
const uploadsNdjson = `\
{"_id":"315318962269342672","type":"Car","name":"Upload at 2024-01-22T19:10:44.048Z","created":"2024-01-22T19:10:44.048+00:00","updated":"2024-01-22T19:10:44.048+00:00","cid":"bafybeieevwnu57cbcp5u6jsy6wxpj2waq5gfq5gc4spss4skpzk34vvxyy","dagSize":8848144,"pins":[{"status":"Pinned","updated":"2024-01-22T19:10:44.048+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaierakuersmo7wndedhwk43e5xwcpzwenuda3dhpcsvkfibewg5gxl7oa"],"deals":[]}
{"_id":"315318962269342671","type":"Car","name":"Upload at 2024-01-22T19:10:23.808Z","created":"2024-01-22T19:10:23.808+00:00","updated":"2024-01-22T19:10:23.808+00:00","cid":"bafybeicmidbuoilkli5w3bttmcndhpikk7zt6ydv3m6rihxl266c546qpi","dagSize":8848085,"pins":[{"status":"Pinned","updated":"2024-01-22T19:10:23.808+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaierayx2sfg6oq5pgazwlzxvi6pcexp7eq5hleut5khpwr4nl4y3rfuhq"],"deals":[]}\
`

await test('can convert one upload to a store/add', async () => {
  const upload = W32023Upload.from(uploadsNdjson.split('\n')[0])
  const adds = [...fromW32023ToW3up.toStoreAdd(upload)]
  assert.deepEqual(adds, [
    {
      size: 8848144,
      link: Link.parse("bagbaierakuersmo7wndedhwk43e5xwcpzwenuda3dhpcsvkfibewg5gxl7oa"),
    }
  ])
})

await test('can convert stream of json to stream of uploads', async () => {
  const ndjson = new ReadableStream({
    start(c) {
      c.enqueue(new TextEncoder().encode(uploadsNdjson))
      c.close()
    }
  })
  const uploads = new W32023UploadsFromNdjson(ndjson)
  let uploadCount = 0;
  for await (const upload of uploads) {
    uploadCount++
  }
  assert.equal(uploadCount, 2)
})

await test('can invoke store/add against mock server', async () => {
  let serverAddsReceived = []
  const server = Server.create({
    id: await ed25519.generate(),
    service: {
      store: {
        add(invocation, ctx) {
          serverAddsReceived.push(invocation)
          return {
            ok: {}
          }
        }
      }
    },
    codec: CAR.inbound,
    validateAuthorization: () => ({ ok: {} }),
  })
  const space = await ed25519.generate()
  const issuer = await ed25519.generate()
  const connection = Client.connect({
    id: issuer,
    codec: CAR.outbound,
    channel: server,
  })
  const upload = W32023Upload.from(uploadsNdjson.split('\n')[0])
  for (const add of fromW32023ToW3up.toStoreAdd(upload)) {
    const receipt = await Store.add.invoke({
      issuer,
      audience: server.id,
      with: space.did(),
      nb: add,
    }).execute(connection)
    assert.deepEqual(receipt.out.ok, {})
  }
  assert.equal(serverAddsReceived.length, 1)
})
