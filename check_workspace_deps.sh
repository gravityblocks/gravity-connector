#!/usr/bin/env bash
# Fail if any workspace dependency is not used by any crate.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
workspace_toml="$root/Cargo.toml"

# Extract dep names: lines in [workspace.dependencies] that look like `name = ...` or `name.something`
deps=$(sed -n '/^\[workspace\.dependencies\]/,/^\[/{
  /^\[/d
  /^#/d
  /^$/d
  /^[a-zA-Z]/!d
  s/\s*[=.].*//
  p
}' "$workspace_toml")

unused=""
for dep in $deps; do
  # gravity-* are internal crates that may be declared in the workspace ahead of use
  case "$dep" in gravity-*) continue ;; esac
  if ! grep -rq "^${dep}[. =]" "$root"/crates/*/Cargo.toml 2>/dev/null; then
    unused="$unused $dep"
  fi
done

if [ -n "$unused" ]; then
  echo "Unused workspace dependencies:"
  for dep in $unused; do echo "  - $dep"; done
  exit 1
fi