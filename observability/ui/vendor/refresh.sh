#!/usr/bin/env bash
# Re-download the console's vendored JS deps from unpkg and verify the
# resulting files against CHECKSUMS.txt. Run from the repo root or this
# directory. Exits non-zero on any download error or checksum mismatch.
#
# Bump versions by editing the URLs below, then re-running this script.
# Commit the new files alongside the new CHECKSUMS.txt so reviewers can
# audit the diff and re-verify locally.

set -euo pipefail

cd "$(dirname "$0")"

declare -a urls=(
  "https://unpkg.com/react@18.3.1/umd/react.production.min.js"
  "https://unpkg.com/react-dom@18.3.1/umd/react-dom.production.min.js"
  "https://unpkg.com/@babel/standalone@7.29.0/babel.min.js"
)

for url in "${urls[@]}"; do
  fname="$(basename "$url")"
  echo "Fetching $fname from $url"
  curl -sSf -o "$fname" "$url"
done

echo "Verifying checksums against CHECKSUMS.txt..."
shasum -a 256 -c CHECKSUMS.txt

echo "OK — vendored files match CHECKSUMS.txt."
echo "If you bumped versions, regenerate CHECKSUMS.txt:"
echo "  shasum -a 256 *.js > CHECKSUMS.txt"
