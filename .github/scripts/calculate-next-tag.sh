#!/usr/bin/env bash
set -euo pipefail

mode="${1:-stable}"
bump="${2:-patch}"

if [[ "$mode" != "stable" && "$mode" != "alpha" ]]; then
  echo "usage: $0 [stable|alpha] [major|minor|patch|none]" >&2
  exit 2
fi

if [[ "$bump" != "major" && "$bump" != "minor" && "$bump" != "patch" && "$bump" != "none" ]]; then
  echo "usage: $0 [stable|alpha] [major|minor|patch|none]" >&2
  exit 2
fi

if [[ "$bump" == "none" ]]; then
  # Stay on the highest version already in flight, released or pre-release, without bumping it.
  # -beta.N is matched purely to keep tags cut before the alpha rename visible here.
  current_version=$(
    git tag --list 'v[0-9]*' \
      | sed -nE 's/^v([0-9]+\.[0-9]+\.[0-9]+)(-(alpha|beta)\.[0-9]+)?$/\1/p' \
      | sort -t. -k1,1n -k2,2n -k3,3n \
      | tail -n 1
  )

  if [[ -z "$current_version" ]]; then
    echo "Error: bump=none needs an existing version tag to continue, found none" >&2
    exit 1
  fi

  next_stable_tag="v${current_version}"
else
  latest_stable_tag=$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -n 1 || true)
  latest_stable_tag=${latest_stable_tag:-v0.0.0}

  raw_version=${latest_stable_tag#v}
  IFS=. read -r major minor patch <<< "$raw_version"

  case "$bump" in
    major) next_stable_tag="v$((major + 1)).0.0" ;;
    minor) next_stable_tag="v${major}.$((minor + 1)).0" ;;
    patch) next_stable_tag="v${major}.${minor}.$((patch + 1))" ;;
  esac
fi

# A released version is final: it can be neither re-cut nor given further alphas.
if git rev-parse -q --verify "refs/tags/${next_stable_tag}" >/dev/null; then
  if [[ "$mode" == "alpha" ]]; then
    echo "Error: cannot cut an alpha for ${next_stable_tag}, it is already released" >&2
  else
    echo "Error: tag ${next_stable_tag} already exists" >&2
  fi
  exit 1
fi

if [[ "$mode" == "stable" ]]; then
  echo "$next_stable_tag"
  exit 0
fi

next_alpha_number=$(
  git tag --list "${next_stable_tag}-alpha.*" \
    | sed -n "s/^${next_stable_tag//./\\.}-alpha\\.\\([0-9][0-9]*\\)$/\\1/p" \
    | sort -n \
    | tail -n 1
)

if [[ -z "$next_alpha_number" ]]; then
  next_alpha_number=1
else
  next_alpha_number=$((next_alpha_number + 1))
fi

echo "${next_stable_tag}-alpha.${next_alpha_number}"
