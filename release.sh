#!/usr/bin/env bash
set -euo pipefail

test -z "$(git status --porcelain)" || { echo "working tree is not clean" >&2; exit 1; }
current="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n1)"
IFS=. read -r major minor patch <<< "$current"
next="$major.$minor.$((patch + 1))"
sed -i "0,/^version = \"$current\"/s//version = \"$next\"/" Cargo.toml
cargo check
git add Cargo.toml Cargo.lock
git commit -m "Release v$next"
git tag "v$next"
git push origin HEAD "v$next"
echo "released v$next"

