#!/usr/bin/env bash
# TrafficSense user-space toolchain installer (no sudo). Idempotent: safe to re-run.
set -euo pipefail
SPARK_VERSION="3.5.9"
SBT_VERSION="1.12.15"
PY_VERSION="3.11"
NODE_MAJOR="22"
OPT="$HOME/opt"; DL="$OPT/.downloads"
# Project root = parent of this script's folder (e.g. /mnt/c/Users/<you>/Desktop/bdaa).
PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The venv stays on the Linux filesystem: importing pyspark/pandas from /mnt/c is very slow.
VENV="$HOME/.venvs/trafficsense-py311"
ENV_FILE="$HOME/.trafficsense_env.sh"
mkdir -p "$OPT" "$DL" "$(dirname "$VENV")"
log(){ printf '\n>>> %s\n' "$*"; }
fetch(){ [ -s "$2" ] || curl -fsSL --retry 3 -o "$2.part" "$1" && mv -f "$2.part" "$2" 2>/dev/null || [ -s "$2" ]; }

log "uv + Python $PY_VERSION"
if ! command -v uv >/dev/null && [ ! -x "$HOME/.local/bin/uv" ]; then
  curl -LsSf https://astral.sh/uv/install.sh | env UV_NO_MODIFY_PATH=1 sh
fi
export PATH="$HOME/.local/bin:$PATH"
uv python install "$PY_VERSION"
[ -x "$VENV/bin/python" ] || uv venv --python "$PY_VERSION" "$VENV"
venv_ver=$("$VENV/bin/python" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
[ "$venv_ver" = "$PY_VERSION" ] || { echo "ERROR: $VENV is Python $venv_ver, expected $PY_VERSION. Not modifying it."; exit 1; }
[ -f "$PROJ/requirements.txt" ] && uv pip install --python "$VENV/bin/python" -r "$PROJ/requirements.txt"

log "Temurin JDK 17"
if [ ! -x "$OPT/jdk-17/bin/java" ]; then
  url=$(curl -sI https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse | awk 'tolower($1)=="location:"{print $2}' | tr -d '\r')
  f="$DL/$(basename "$url")"
  fetch "$url" "$f"
  expected=$(curl -fsL "$url.sha256.txt" | awk '{print $1}')
  echo "$expected  $f" | sha256sum -c -
  dest="$OPT/temurin-17"; rm -rf "$dest"; mkdir -p "$dest"
  tar -xzf "$f" -C "$dest" --strip-components=1
  ln -sfn "$dest" "$OPT/jdk-17"
fi
"$OPT/jdk-17/bin/java" -version 2>&1 | head -1

log "Apache Spark $SPARK_VERSION (hadoop3)"
if [ ! -x "$OPT/spark/bin/spark-submit" ]; then
  name="spark-$SPARK_VERSION-bin-hadoop3"
  f="$DL/$name.tgz"
  fetch "https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/$name.tgz" "$f" \
    || fetch "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$name.tgz" "$f"
  expected=$(curl -fsL "https://downloads.apache.org/spark/spark-$SPARK_VERSION/$name.tgz.sha512" \
    || curl -fsL "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$name.tgz.sha512")
  expected=$(echo "$expected" | sed 's/.*://' | tr -d ' \n\r' | tr 'A-F' 'a-f' | grep -oE '[0-9a-f]{128}')
  actual=$(sha512sum "$f" | awk '{print $1}')
  [ "$expected" = "$actual" ] || { echo "Spark checksum mismatch"; rm -f "$f"; exit 1; }
  echo "Spark sha512 OK"
  dest="$OPT/$name"; rm -rf "$dest"; mkdir -p "$dest"
  tar -xzf "$f" -C "$dest" --strip-components=1
  ln -sfn "$dest" "$OPT/spark"
fi

log "sbt launcher $SBT_VERSION"
if [ "$(cat "$OPT/sbt/.ts_version" 2>/dev/null)" != "$SBT_VERSION" ]; then
  f="$DL/sbt-$SBT_VERSION.tgz"
  fetch "https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz" "$f"
  rm -rf "$OPT/sbt"; mkdir -p "$OPT/sbt"
  tar -xzf "$f" -C "$OPT/sbt" --strip-components=1
  echo "$SBT_VERSION" > "$OPT/sbt/.ts_version"
fi

log "nvm + Node $NODE_MAJOR LTS"
export NVM_DIR="$HOME/.nvm"
if [ ! -s "$NVM_DIR/nvm.sh" ]; then
  tag=$(curl -sI https://github.com/nvm-sh/nvm/releases/latest | awk 'tolower($1)=="location:"{print $2}' | tr -d '\r' | sed 's#.*/tag/##')
  curl -fsSL "https://raw.githubusercontent.com/nvm-sh/nvm/$tag/install.sh" | PROFILE=/dev/null bash
fi
set +u; . "$NVM_DIR/nvm.sh"; nvm install "$NODE_MAJOR" --no-progress >/dev/null; nvm alias default "$NODE_MAJOR" >/dev/null; set -u
echo "node $(node -v)"

log "Environment file $ENV_FILE"
cat > "$ENV_FILE" <<EOF
# TrafficSense toolchain environment. Source from shells and scripts: . "\$HOME/.trafficsense_env.sh"
export TRAFFICSENSE_HOME="$PROJ"
export TRAFFICSENSE_VENV="$VENV"
export JAVA_HOME="\$HOME/opt/jdk-17"
export SPARK_HOME="\$HOME/opt/spark"
export PYSPARK_PYTHON="\$TRAFFICSENSE_VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="\$PYSPARK_PYTHON"
export NVM_DIR="\$HOME/.nvm"
[ -s "\$NVM_DIR/nvm.sh" ] && . "\$NVM_DIR/nvm.sh"
export PATH="\$TRAFFICSENSE_VENV/bin:\$HOME/.local/bin:\$JAVA_HOME/bin:\$SPARK_HOME/bin:\$HOME/opt/sbt/bin:\$PATH"
alias ts='cd "\$TRAFFICSENSE_HOME"'
EOF
for rc in "$HOME/.bashrc" "$HOME/.profile"; do
  touch "$rc"
  sed -i '/# >>> trafficsense >>>/,/# <<< trafficsense <<</d' "$rc"
  printf '# >>> trafficsense >>>\n[ -f "$HOME/.trafficsense_env.sh" ] && . "$HOME/.trafficsense_env.sh"\n# <<< trafficsense <<<\n' >> "$rc"
done
rm -rf "$DL"
log "DONE"