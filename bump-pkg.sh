#!/usr/bin/env bash
# Points the Arch package at a published release: ./bump-pkg.sh 0.2.1
# Run it once the release workflow has finished. It leaves committing to you.
set -euo pipefail

version="${1:-}"
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "usage: $0 VERSION, for example $0 0.2.1" >&2; exit 2; }

cd "$(dirname "$(readlink -f "$0")")/packaging/arch"

url=$(sed -n 's/^url="\(.*\)"$/\1/p' PKGBUILD)
for arch in amd64 arm64; do
	asset="$url/releases/download/v$version/rushfiles-v$version-linux-$arch.tar.gz"
	curl -sfIL -o /dev/null "$asset" || { echo "not published yet: $asset" >&2; exit 1; }
done

sed -i "s/^pkgver=.*/pkgver=$version/; s/^pkgrel=.*/pkgrel=1/" PKGBUILD
updpkgsums
makepkg --printsrcinfo > .SRCINFO
rm -f rushfiles-v*.tar.gz
echo "packaging/arch now points at v$version"
