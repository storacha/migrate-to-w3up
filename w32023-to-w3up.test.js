import { describe, test } from 'node:test'
import { W32023Upload } from './w32023.js'

/** example uploads from `w3 list --json` */
const uploadsNdjson = `\
{"_id":"315318962269342672","type":"Car","name":"Upload at 2024-01-22T19:10:44.048Z","created":"2024-01-22T19:10:44.048+00:00","updated":"2024-01-22T19:10:44.048+00:00","cid":"bafybeieevwnu57cbcp5u6jsy6wxpj2waq5gfq5gc4spss4skpzk34vvxyy","dagSize":8848144,"pins":[{"status":"Pinned","updated":"2024-01-22T19:10:44.048+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaierakuersmo7wndedhwk43e5xwcpzwenuda3dhpcsvkfibewg5gxl7oa"],"deals":[]}
{"_id":"315318962269342671","type":"Car","name":"Upload at 2024-01-22T19:10:23.808Z","created":"2024-01-22T19:10:23.808+00:00","updated":"2024-01-22T19:10:23.808+00:00","cid":"bafybeicmidbuoilkli5w3bttmcndhpikk7zt6ydv3m6rihxl266c546qpi","dagSize":8848085,"pins":[{"status":"Pinned","updated":"2024-01-22T19:10:23.808+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaierayx2sfg6oq5pgazwlzxvi6pcexp7eq5hleut5khpwr4nl4y3rfuhq"],"deals":[]}\
`

test('can convert one upload to a store/add', async () => {
  const upload = W32023Upload.from(uploadsNdjson.split('\n')[0])
  console.log('upload', { upload })
})
