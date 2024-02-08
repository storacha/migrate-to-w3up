# migrate-to-w3up ⁂

Migrate files to web3.storage.

## Supported Migration Sources

* https://old.web3.storage/
  * bring a `WEB3_TOKEN` environment variable from https://old.web3.storage/tokens/

Please file an issue to suggest a migration source that you would use.

## Installation

```shell
npm install -g migrate-to-w3up
```

## Usage

Warning!
* this is alpha software and we're still testing it with big uploads. If that sounds scary, check back for the v1 release when we've done a couple migrations in production.
* if a migration errors part of the way through
    * you can't undo the partial migration
    * retrying the migration will start from the beginning, but anything already migrated should be super fast because it will just verify that the w3up space already has an Upload with the right cid.
* this can migrate a lot of data rapidly! be sure you really want to add everything from the migration source to the destination.
    * to be extra careful, use filter mode and export your uploads from your source to a file. inspect the file to make sure it only contains uploads you want to migrate. Then pipe that file to `migrate-to-w3up` filter mode.
* the space you migrate to must have a [w3 storage provider][] attached. This usually means the storage provider will charge the account that added the storage provider to the space for any migrated files.

### space wizard 🧙‍♀️ mode

```
migrate-to-w3up
```

You will then see a series of prompts asking you:
* where you want to migrate to, e.g. pick a [w3up space][]
* where you want to migrate from, e.g.
  * old.web3.storage uploads list from `WEB3_TOKEN` environment variable, if set and user confirms
  * old.web3.storage list from `WEB3_TOKEN` that user inputs into prompt

### filter mode

You can also use `migrate-to-w3up` as a [unix filter][].

Just pipe in [ndjson][] of old.web3.storage Uploads objects.

```shell
# gets current w3cli space (requires jq).
# copypasta from `w3 space ls` to pick another one,
# or do `w3 space use <space>` first
space=$(w3 space info --json | jq '.did' -r)

# define a command to get uploads as migration source
alias w32023-export='npx @web3-storage/w3@latest list --json'

migrate-to-w3up --space="$space" \
< <(w32023-export) \
| tee -a /tmp/migrate-to-w3up.$(date +%s).log
# include the previous line only if you want a logfile

# this also works
# jq optional but useful for pretty printing
# https://jqlang.github.io/jq/
w32023-export | migrate-to-w3up --space="$space" | jq
```

## How it Works

### [w32023-to-w3up.js](./w32023-to-w3up.js)

* exports a `migrate` function that runs a migration, returning an `AsyncIterable` of `MigratedUpload<W32023Upload>` that includes [ucanto receipts][] for every request sent to w3up as part of the migration.

### [migrate-to-w3up.js](./migrate-to-w3up.js)

* this is the main [bin]() script that runs when the package is globally installed and someone runs `migrate-to-w3up`
* it reads cli flags to build options for `migrate`. If some options aren't provided as flags, and there is an interactive terminal, prompt the terminal for the options required to start a migration, e.g. selecting a `source` of uploads and `destination` (e.g. space DID).
* it calls `migrate` from w32023-to-w3up.js and interprets the results as CLI output
  * it writes [ndjson][] to stdout, and any errors/warnings to stderr.
  * it is recommended to pipe the output of this to a file so it can be explored later to check receipts or explore logs for debugging. But you can also pipe to `jq` to pretty print the ndjson output for ad-hoc exploration.

<!-- references -->

[ndjson]: https://en.wikipedia.org/wiki/JSON_streaming
[unix filter]: https://en.wikipedia.org/wiki/Unix_philosophy#Mike_Gancarz:_The_UNIX_Philosophy
[w3up space]: https://web3.storage/docs/how-to/create-space/
[w3 storage provider]: https://github.com/web3-storage/specs/blob/main/w3-provider.md
[ucanto receipts]: https://github.com/web3-storage/ucanto/pull/266
