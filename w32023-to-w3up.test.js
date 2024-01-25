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
{"_id":"1","type":"Car","name":"Upload at 2024-01-19T04:40:04.490Z","created":"2024-01-19T04:40:04.49+00:00","updated":"2024-01-19T04:40:04.49+00:00","cid":"bafybeihtddvvufnzdcetubq5mbv2rvgjchlipf6y7esei5qzg4r7re7rju","dagSize":2949303,"pins":[{"status":"Pinned","updated":"2024-01-19T04:40:04.49+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaieraclriozt34fk5ej3aa7k67es2hyq5zyc3ohivgbee4qeyyeroqb4a"],"deals":[]}\
`

// add later 

await test('can convert one upload to a store/add', async () => {
  const upload = W32023Upload.from(uploadsNdjson.split('\n').filter(Boolean)[0])
  const adds = []
  for await (const a of fromW32023ToW3up.toStoreAdd(upload)) { adds.push(a) }
  assert.equal(adds.length, 1)
  assert.equal(adds[0].size, 2949554)
  assert.equal(adds[0].link.toString(), Link.parse("bagbaieraclriozt34fk5ej3aa7k67es2hyq5zyc3ohivgbee4qeyyeroqb4a").toString())
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
    assert.ok(typeof upload.cid, 'string')
  }
  assert.equal(uploadCount, 1)
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
  for await (const add of fromW32023ToW3up.toStoreAdd(upload)) {
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
