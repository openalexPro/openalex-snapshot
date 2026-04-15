# Command: download

Sync OpenAlex snapshot from S3 via AWS CLI.

Validation is a separate step via `verify_download`.

## Usage

```bash
openalex-snapshot download --root-dir /data
```

Default sync intent:

```bash
aws s3 sync --delete s3://openalex /data/openalex-snapshot --no-sign-request
```
