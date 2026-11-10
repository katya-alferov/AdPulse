#!/bin/sh
# Starts SeaweedFS with an S3 API on port 8333.
# The S3 credentials are read from environment variables (from .env) and
# written to a config file at runtime, so no secret is stored in Git.
set -eu
: "${S3_ACCESS_KEY:?S3_ACCESS_KEY is required}"
: "${S3_SECRET_KEY:?S3_SECRET_KEY is required}"

cat > /tmp/s3.json <<JSON
{
  "identities": [
    {
      "name": "adpulse",
      "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
JSON

# server = master + volume + filer in one process (perfect for a laptop)
exec weed server -dir=/data -ip=seaweedfs -ip.bind=0.0.0.0 \
  -master.volumeSizeLimitMB=256 -volume.max=0 \
  -s3 -s3.port=8333 -s3.config=/tmp/s3.json
