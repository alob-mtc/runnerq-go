#!/usr/bin/env bash
# Re-download the console's vendored JS deps from unpkg, verify SHA-256
# against CHECKSUMS.txt, and print the SHA-384 SRI hashes that need to
# match runnerq-console.html's integrity= attributes.
#
# Bump versions by editing the URLs below, then re-running this script.
# Commit the new files alongside the new CHECKSUMS.txt and the updated
# integrity= hashes in runnerq-console.html so reviewers can audit the
# diff and re-verify locally.
#
# Run from the repo root or from this directory.

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

echo
echo "Verifying SHA-256 against CHECKSUMS.txt..."
shasum -a 256 -c CHECKSUMS.txt

echo
echo "SRI hashes for runnerq-console.html integrity= attributes:"
for url in "${urls[@]}"; do
  fname="$(basename "$url")"
  hash=$(openssl dgst -sha384 -binary "$fname" | openssl base64 -A)
  printf "  %-32s sha384-%s\n" "$fname" "$hash"
done

echo
echo "If you bumped versions:"
echo "  1. Regenerate CHECKSUMS.txt:  shasum -a 256 *.js > CHECKSUMS.txt"
echo "  2. Update the integrity= attributes in"
echo "     observability/ui/runnerq-console.html with the SRI hashes above."
