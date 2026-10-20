#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
plugin_dir="${HARNESSX_PROTO_PLUGIN_DIR:-$HOME/go/bin}"

for tool in protoc protoc-gen-go protoc-gen-go-grpc; do
  if ! command -v "$tool" >/dev/null 2>&1 && [[ ! -x "$plugin_dir/$tool" ]]; then
    printf 'missing required protobuf tool: %s\n' "$tool" >&2
    exit 1
  fi
done

mkdir -p "$repo_root/gen/go"
PATH="$plugin_dir:$PATH" protoc \
  -I "$repo_root/proto" \
  --go_out="$repo_root/gen/go" --go_opt=paths=source_relative \
  --go-grpc_out="$repo_root/gen/go" --go-grpc_opt=paths=source_relative \
  "$repo_root"/proto/harnessx/v1/*.proto
