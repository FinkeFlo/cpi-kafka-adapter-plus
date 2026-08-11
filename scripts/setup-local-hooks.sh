#!/usr/bin/env bash
set -euo pipefail

git config core.hooksPath .githooks
git config commit.template .gitmessage.txt

echo "Configured for this repository:"
echo "  - core.hooksPath = .githooks"
echo "  - commit.template = .gitmessage.txt"
