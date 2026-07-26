#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"

node -e 'const [major, minor] = process.versions.node.split(".").map(Number); if (major < 22 || (major === 22 && minor < 12)) { console.error(`Node.js 22.12 or later is required. Current version: ${process.versions.node}`); process.exit(1); }'

npm ci
npm run build-linux-AppImage
