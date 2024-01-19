assign fixture to $upload variable
```shell
upload='{"_id":"1","type":"Car","name":"Upload at 2024-01-19T04:40:04.490Z","created":"2024-01-19T04:40:04.49+00:00","updated":"2024-01-19T04:40:04.49+00:00","cid":"bafybeihtddvvufnzdcetubq5mbv2rvgjchlipf6y7esei5qzg4r7re7rju","dagSize":2949303,"pins":[{"status":"Pinned","updated":"2024-01-19T04:40:04.49+00:00","peerId":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","peerName":"elastic-ipfs","region":null}],"parts":["bagbaieraclriozt34fk5ej3aa7k67es2hyq5zyc3ohivgbee4qeyyeroqb4a"],"deals":[]}'
```

`upload-to-files.js` will fetch the `parts` from the provided upload, decode them, and write to local fs
```shell
⚡ node upload-to-file.js --json "$upload"
piped to bafybeihtddvvufnzdcetubq5mbv2rvgjchlipf6y7esei5qzg4r7re7rju/GOPR0787.JPG
```


```shell
⚡ (echo $upload; echo $upload) | migrate-from-w32023-to-mime --fetchParts > bengo.mime
wrote /dev/stdout

⚡ cat bengo.mime | head -n35
Content-Type: multipart/mixed; boundary=kf7cqzqdegb; type=multipart/mixed

--kf7cqzqdegb
content-disposition: attachment; filename="Upload at 2024-01-19T04:40:04.490Z.bafybeihtddvvufnzdcetubq5mbv2rvgjchlipf6y7esei5qzg4r7re7rju.ipfs"
content-type: application/vnd.web3.storage.car+json;version=2023.old.web3.storage
content-id: bagaaieradpqpmczlz6gwmwew34qjobdhf5k6qlcy4q4wbvocrnn3qhpfhymq

{
  "_id": "1",
  "type": "Car",
  "name": "Upload at 2024-01-19T04:40:04.490Z",
  "created": "2024-01-19T04:40:04.49+00:00",
  "updated": "2024-01-19T04:40:04.49+00:00",
  "cid": "bafybeihtddvvufnzdcetubq5mbv2rvgjchlipf6y7esei5qzg4r7re7rju",
  "dagSize": 2949303,
  "pins": [
    {
      "status": "Pinned",
      "updated": "2024-01-19T04:40:04.49+00:00",
      "peerId": "bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm",
      "peerName": "elastic-ipfs",
      "region": null
    }
  ],
  "parts": [
    "bagbaieraclriozt34fk5ej3aa7k67es2hyq5zyc3ohivgbee4qeyyeroqb4a"
  ],
  "deals": []
}

--kf7cqzqdegb
content-id: bafybeihtddvvufnzdcetubq5mbv2rvgjchlipf6y7esei5qzg4r7re7rju
content-type: application/vnd.ipld.car
content-transfer-encoding: BASE64
# much base64(CAR) on next line
```
