#!/usr/bin/env bash
# Build the ESA archive locally using Docker (linux/amd64) for exact CI parity.
#
# Usage:
#   scripts/build-esa.sh
#
# Requirements:
#   - Docker running with linux/amd64 support (Rosetta on Apple Silicon)
#
# Output:
#   The generated .esa is printed to stdout as the last line of the script.
#   It lives inside target/ and can be deployed directly to SAP CPI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
M2_CACHE="${HOME}/.m2"

echo "Building ESA in Docker (linux/amd64) ..."
echo "  Project : $PROJECT_DIR"
echo "  M2 cache: $M2_CACHE"
echo ""

# Remove target/ from the host before the container's `mvn clean` gets to it.
# Finder and Spotlight re-create .DS_Store inside a directory the moment its
# contents change, so the container could delete a .DS_Store, find the directory
# non-empty again an instant later and fail the whole build with
# "Failed to clean project: Failed to delete /workspace/target/...".
# With target/ already gone, the clean inside the container is a no-op.
if [ -d "$PROJECT_DIR/target" ]; then
  rm -rf "$PROJECT_DIR/target"
fi

docker run --rm \
  --platform linux/amd64 \
  -v "$PROJECT_DIR":/workspace \
  -v "$M2_CACHE":/root/.m2 \
  -w /workspace \
  maven:3.9-eclipse-temurin-17 \
  mvn -B clean install -DskipITs

ESA="$(find "$PROJECT_DIR/target" -name "*.esa" | head -1)"
if [ -z "$ESA" ]; then
  echo "ERROR: No .esa file found under target/" >&2
  exit 1
fi

echo ""
echo "ESA ready: $ESA"
