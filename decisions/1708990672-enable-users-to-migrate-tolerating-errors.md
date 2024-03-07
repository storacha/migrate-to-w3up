# enable users to migrate tolerating errors

Authors
* [bengo](https://bengo.is)

## Context

We are trying to enable this user story:
* As a user of old.web3.storage who wants to migrate to w3 up, I can use migrate-to-w3up to migrate all my uploads from old.web3.storage to a w3up space, so I can stop using old.web3.storage but still expect all my old uploads to be available on IPFS.

To some extent we have, but it leaves a lot of room for improvement in really big migration runs. For example, I have been testing an account with more than 370k uploads. This means at least that many invocations, and at that scale we're likely to face 'normal errors' that we may not be able to rely on avoiding alltogether.

When these errors happen now, the migration process halts due to an unexpected error. This means when you do a big migration run an encounter a 'normal error', you may not find out for a couple hours, then see the error later, and there isn't a way to continue migrating from the last upload that worked.

## Decision

### Requirement 1

`migrate-to-w3up` MUST be able to run a migration of an old.web3.storage account in such a way that, when encountering an error, the migration process tolerates it, logs the error, and moves on to attempting migration of the rest of the uploads.

These logs will include
* migration run start datetime
* migration run stop datetime
* log for any source uploads that could not be migrated (e.g. due to intermittent network failure)

Rationale: This will make it possible for any users of `migrate-to-w3up` to kick off migration of a huge old.web3.storage account, check back over time on progress, and when the process ends, be able to report on how many of the source uploads were migrated, how many failed to be migrated, and the cause of any failure.

### Requirement 2

`migrate-to-w3up` must show a progress indicator for migrations of old.web3.storage accounts.

Rationale: this makes it explicit how to understand how long the migration might take, whether it's stuck, etc. Without this it's almost impossible to set expectations for duration of a migration run.

### Ideation

Example invocation after this change:
```shell
# (with WEB3_TOKEN env var set which selects source uploads)
# note stderr is redirected
⚡️ migration=w3up-migration-$(date +%s); migrate-to-w3up --space <space.did> --log-failures /tmp/$migration.failures.ndjson
# terminal would show progress and any errors
# migration failures will be appended to /tmp/$migration.failures.ndjson
```

`migrate-to-w3up` SHOULD be able to run a second migration pass to retry all failed uploads using only the log file `/tmp/$migration.failures.ndjson`

## Implementation

* https://github.com/web3-storage/migrate-to-w3up/pull/6

## Changelog

* 2024-02-26: initial draft of this document
