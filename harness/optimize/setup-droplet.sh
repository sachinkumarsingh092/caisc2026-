#!/usr/bin/env bash
# One-time bootstrap on a fresh DO Ubuntu droplet. Run ON the droplet.
#   bash /root/caisc2026/harness/optimize/setup-droplet.sh
#
# Idempotent: re-running is safe. Skips installs that already succeeded.
set -euo pipefail

REPO="${REPO:-/root/caisc2026}"
GO_VERSION="${GO_VERSION:-1.26.3}"

cd "$REPO"

echo "==[ 1/8 ]== apt deps"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
  build-essential git curl wget tmux jq rsync \
  openjdk-17-jdk-headless \
  python3.12 python3.12-venv python3-pip \
  texlive-latex-base texlive-latex-recommended \
  >/dev/null

echo "==[ 2/8 ]== Go $GO_VERSION"
if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "go${GO_VERSION}"; then
  curl -sSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tgz
  rm -rf /usr/local/go
  tar -C /usr/local -xzf /tmp/go.tgz
  rm -f /tmp/go.tgz
fi
# Make Go on PATH for this shell and persistently
export PATH=/usr/local/go/bin:$PATH
grep -q '/usr/local/go/bin' /root/.bashrc || \
  echo 'export PATH=/usr/local/go/bin:$PATH' >> /root/.bashrc
go version

echo "==[ 3/8 ]== uv"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH=$HOME/.local/bin:$PATH
grep -q '\.local/bin' /root/.bashrc || \
  echo 'export PATH=$HOME/.local/bin:$PATH' >> /root/.bashrc
uv --version

echo "==[ 4/8 ]== Python venv + OpenEvolve"
if [ ! -d "$REPO/.venv" ]; then
  python3.12 -m venv "$REPO/.venv"
fi
uv pip install --python "$REPO/.venv/bin/python" openevolve pypdf 2>&1 | tail -3
"$REPO/.venv/bin/openevolve-run" --help >/dev/null

echo "==[ 5/8 ]== TLA+ jars"
mkdir -p "$REPO/raft-upstream/tla/jars"
[ -f "$REPO/raft-upstream/tla/jars/tla2tools.jar" ] || \
  curl -sSL -o "$REPO/raft-upstream/tla/jars/tla2tools.jar" \
       https://nightly.tlapl.us/dist/tla2tools.jar
[ -f "$REPO/raft-upstream/tla/jars/CommunityModules-deps.jar" ] || \
  curl -sSL -L -o "$REPO/raft-upstream/tla/jars/CommunityModules-deps.jar" \
       https://github.com/tlaplus/CommunityModules/releases/latest/download/CommunityModules-deps.jar

echo "==[ 6/8 ]== restore baseline raft.go (just in case)"
/bin/cp "$REPO/raft-upstream/raft.go.baseline" "$REPO/raft-upstream/raft.go"

echo "==[ 7/8 ]== upstream raft test suite"
(cd "$REPO/raft-upstream" && go test -count=1 -timeout 180s ./... 2>&1 | tail -10)

echo "==[ 8/8 ]== DST harness smoke test"
(cd "$REPO/harness/dst" && go mod tidy >/dev/null && \
  go build -buildvcs=false -o /tmp/dst ./cmd/dst)
/tmp/dst -seed=1 -ticks=500 -nodes=3 2>/dev/null

echo ""
echo "================================================================"
echo "SETUP COMPLETE on $(hostname)"
echo "  Go:        $(go version | cut -d' ' -f3)"
echo "  Python:    $(python3.12 --version)"
echo "  OpenEvolve in $REPO/.venv"
echo "  TLA+ jars in $REPO/raft-upstream/tla/jars/"
echo "================================================================"
echo ""
echo "Next: drop the API key into $REPO/.env"
echo "      scp from your Mac:  scp .env root@<this-host>:$REPO/.env"
