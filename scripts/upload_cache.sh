#!/usr/bin/env bash
# Upload a pspcz_analyzer cache archive to a remote host, resumably and
# verifiably. Companion to scripts/build_and_backup_cache.py, which produces
# the pspcz-cache-<timestamp>.tar.gz archives this script ships.
#
# The transfer uses rsync --partial --append-verify inside an auto-retry
# loop, so a dropped connection resumes where it left off instead of
# restarting. Designed to run unattended overnight on a slow uplink:
#
#   tmux new -s cache-upload
#   ./scripts/upload_cache.sh                       # newest archive → host "master"
#   # Ctrl-b d to detach; reattach later with: tmux attach -t cache-upload
#
# Usage:
#   ./scripts/upload_cache.sh [TARBALL] [REMOTE_HOST] [OPTIONS]
#
# Arguments:
#   TARBALL                 Archive to upload (default: newest ./pspcz-cache-*.tar.gz)
#   REMOTE_HOST             SSH host alias or user@host (default: $PSPCZ_REMOTE or "master")
#
# Options:
#   --remote-dir DIR        Remote staging directory (default: ~/cache-upload)
#   --extract               After verified upload, extract into --remote-cache-dir
#   --remote-cache-dir DIR  Absolute remote cache dir to extract into (PSPCZ_CACHE_DIR)
#   --clobber-state         With --extract: also overwrite runtime_config.json and
#                           pipeline_history.json (default: preserved — the archive
#                           carries the build machine's copies, which would silently
#                           replace production's LLM/provider configuration)
#   --attempts N            Max transfer attempts before giving up (default: 10)
#   --dry-run               Run preflight and a simulated rsync; change nothing
#   --help, -h              Show this help
#
# Every run is logged to ./upload-cache-<timestamp>.log (tee'd to the terminal).

set -euo pipefail

# ── Defaults ────────────────────────────────────────────────────────────
TARBALL=""
REMOTE_HOST="${PSPCZ_REMOTE:-master}"
REMOTE_DIR="~/cache-upload"
EXTRACT=0
REMOTE_CACHE_DIR=""
CLOBBER_STATE=0
MAX_ATTEMPTS=10
DRY_RUN=0

SSH_OPTS="-o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o BatchMode=yes"
RSYNC_TIMEOUT=60
BACKOFF_BASE=30          # seconds; sleep = min(BACKOFF_BASE * attempt, BACKOFF_CAP)
BACKOFF_CAP=120
DISK_HEADROOM_NUM=11     # require free >= size * NUM / DEN (110 %)
DISK_HEADROOM_DEN=10

# ── Argument parsing ────────────────────────────────────────────────────
usage() {
    # Print the leading comment block (minus the shebang) as help text.
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote-dir)
            [[ $# -ge 2 ]] || die "--remote-dir requires a value"
            REMOTE_DIR="$2"; shift 2 ;;
        --extract)
            EXTRACT=1; shift ;;
        --remote-cache-dir)
            [[ $# -ge 2 ]] || die "--remote-cache-dir requires a value"
            REMOTE_CACHE_DIR="$2"; shift 2 ;;
        --clobber-state)
            CLOBBER_STATE=1; shift ;;
        --attempts)
            [[ $# -ge 2 ]] || die "--attempts requires a value"
            MAX_ATTEMPTS="$2"; shift 2 ;;
        --dry-run)
            DRY_RUN=1; shift ;;
        --help|-h)
            usage; exit 0 ;;
        -*)
            die "Unknown option: $1 (see --help)" ;;
        *)
            if [[ -z "$TARBALL" ]]; then
                TARBALL="$1"
            elif [[ "$REMOTE_HOST" == "${PSPCZ_REMOTE:-master}" ]]; then
                REMOTE_HOST="$1"
            else
                die "Unexpected argument: $1 (see --help)"
            fi
            shift ;;
    esac
done

[[ "$MAX_ATTEMPTS" =~ ^[1-9][0-9]*$ ]] || die "--attempts must be a positive integer"
[[ "$EXTRACT" -eq 0 || -n "$REMOTE_CACHE_DIR" ]] \
    || die "--extract requires --remote-cache-dir <absolute path>"

# Reject paths that would break the single-quoting used in remote commands.
[[ "$REMOTE_DIR" != *"'"* ]] || die "--remote-dir must not contain a single quote"
[[ "$REMOTE_CACHE_DIR" != *"'"* ]] || die "--remote-cache-dir must not contain a single quote"

# ── Logging ─────────────────────────────────────────────────────────────
LOG_FILE="./upload-cache-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

log() {
    echo "[$(date +%H:%M:%S)] $*"
}

trap 'log "FAILED — see $LOG_FILE"' ERR

# ── Helpers ─────────────────────────────────────────────────────────────
sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

human_size() {
    awk -v b="$1" 'BEGIN {
        split("B KB MB GB TB", u, " ");
        i = 1;
        while (b >= 1024 && i < 5) { b /= 1024; i++ }
        printf "%.1f %s", b, u[i];
    }'
}

remote() {
    # shellcheck disable=SC2086  # SSH_OPTS must word-split
    ssh $SSH_OPTS "$REMOTE_HOST" "$@"
}

# ── Preflight ───────────────────────────────────────────────────────────
preflight() {
    log "Preflight: $TARBALL → $REMOTE_HOST:$REMOTE_DIR"

    [[ -s "$TARBALL" ]] || die "Tarball not found or empty: $TARBALL"
    TARBALL_NAME=$(basename "$TARBALL")
    TARBALL_SIZE=$(stat -c %s "$TARBALL" 2>/dev/null || stat -f %z "$TARBALL")
    log "  archive: $TARBALL_NAME ($(human_size "$TARBALL_SIZE"))"

    command -v rsync >/dev/null 2>&1 || die "rsync not found locally"
    remote 'command -v rsync >/dev/null 2>&1' || die "rsync not found on $REMOTE_HOST"
    remote 'command -v sha256sum >/dev/null 2>&1' || die "sha256sum not found on $REMOTE_HOST"

    # Resolve ~ against the REMOTE home now — tilde inside the single quotes
    # used by later remote commands would not be expanded by the remote shell.
    REMOTE_HOME=$(remote 'echo "$HOME"')
    if [[ "$REMOTE_DIR" == "~" ]]; then
        REMOTE_DIR="$REMOTE_HOME"
    elif [[ "$REMOTE_DIR" == "~/"* ]]; then
        REMOTE_DIR="$REMOTE_HOME/${REMOTE_DIR#"~/"}"
    fi
    log "  remote staging dir: $REMOTE_DIR"

    log "  computing local checksum…"
    LOCAL_SHA=$(sha256_of "$TARBALL")
    log "  sha256: $LOCAL_SHA"

    remote "mkdir -p '$REMOTE_DIR'"
    REMOTE_FREE=$(remote "df --output=avail -B1 '$REMOTE_DIR' | tail -1 | tr -d ' '")
    [[ "$REMOTE_FREE" =~ ^[0-9]+$ ]] || die "Could not read remote free space"
    REQUIRED=$(( TARBALL_SIZE * DISK_HEADROOM_NUM / DISK_HEADROOM_DEN ))
    if (( REMOTE_FREE < REQUIRED )); then
        die "Insufficient remote disk: $(human_size "$REMOTE_FREE") free, need >= $(human_size "$REQUIRED")"
    fi
    log "  remote disk: $(human_size "$REMOTE_FREE") free (need $(human_size "$REQUIRED")) — OK"

    if [[ "$EXTRACT" -eq 1 ]]; then
        [[ "$REMOTE_CACHE_DIR" == /* ]] || die "--remote-cache-dir must be an absolute path"
        [[ "$REMOTE_CACHE_DIR" != "/" ]] || die "--remote-cache-dir must not be /"
        [[ "$REMOTE_CACHE_DIR" != "$REMOTE_HOME" ]] \
            || die "--remote-cache-dir must not be the remote home directory"
        remote 'command -v tar >/dev/null 2>&1' || die "tar not found on $REMOTE_HOST"
        log "  extract target: $REMOTE_CACHE_DIR (state files preserved: $([[ "$CLOBBER_STATE" -eq 1 ]] && echo no || echo yes))"
    fi

    log "Preflight passed."
}

# ── Transfer ────────────────────────────────────────────────────────────
transfer() {
    local rsync_opts=(
        --partial --append-verify --progress
        --timeout="$RSYNC_TIMEOUT"
        -e "ssh $SSH_OPTS"
    )
    [[ "$DRY_RUN" -eq 1 ]] && rsync_opts+=(--dry-run)

    local attempt=1
    log "Transferring (attempt $attempt/$MAX_ATTEMPTS)…"
    # shellcheck disable=SC2086  # rsync_opts array is fine; quoting kept explicit
    until rsync "${rsync_opts[@]}" "$TARBALL" "$REMOTE_HOST:$REMOTE_DIR/"; do
        if (( attempt >= MAX_ATTEMPTS )); then
            die "Transfer still failing after $MAX_ATTEMPTS attempts — giving up (partial file kept on remote; re-run resumes)"
        fi
        local delay=$(( BACKOFF_BASE * attempt ))
        if (( delay > BACKOFF_CAP )); then
            delay=$BACKOFF_CAP
        fi
        log "Transfer failed — retrying in ${delay}s (attempt $((attempt + 1))/$MAX_ATTEMPTS; resumes via --append-verify)…"
        sleep "$delay"
        attempt=$((attempt + 1))
    done
    log "Transfer complete."
}

# ── Verify ──────────────────────────────────────────────────────────────
verify() {
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log "Dry run — skipping checksum verification."
        return
    fi
    log "Verifying remote checksum…"
    local remote_sha
    remote_sha=$(remote "sha256sum '$REMOTE_DIR/$TARBALL_NAME' | cut -d' ' -f1")
    if [[ "$remote_sha" != "$LOCAL_SHA" ]]; then
        die "Checksum MISMATCH (local $LOCAL_SHA, remote $remote_sha) — delete $REMOTE_DIR/$TARBALL_NAME and re-run"
    fi
    log "VERIFIED: $REMOTE_HOST:$REMOTE_DIR/$TARBALL_NAME matches local sha256."
}

# ── Extract ─────────────────────────────────────────────────────────────
extract() {
    [[ "$EXTRACT" -eq 1 ]] || return 0
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log "Dry run — skipping extraction into $REMOTE_CACHE_DIR."
        return
    fi

    log "────────────────────────────────────────────────────────────"
    log "WARNING: stop the application before extracting — the frontend"
    log "file-watcher hot-reloads on cache changes and would reload from"
    log "partially-written files. On the remote host:"
    log "    docker compose stop    (or: systemctl stop pspcz-analyzer)"
    log "────────────────────────────────────────────────────────────"
    if [[ -t 0 ]]; then
        read -rp "App stopped — extract now? [y/N] " confirm
        [[ "$confirm" =~ ^[Yy]$ ]] || die "Aborted before extraction (archive is uploaded and verified; extract later manually)"
    else
        log "Non-interactive shell — proceeding (pass --extract only when the app is stopped)."
    fi

    local excludes=()
    if [[ "$CLOBBER_STATE" -eq 0 ]]; then
        excludes=(--exclude=runtime_config.json --exclude=pipeline_history.json)
        log "Preserving production runtime_config.json / pipeline_history.json (override: --clobber-state)."
    fi

    remote "mkdir -p '$REMOTE_CACHE_DIR'"
    remote "tar xzf '$REMOTE_DIR/$TARBALL_NAME' ${excludes[*]:-} -C '$REMOTE_CACHE_DIR'"

    local missing=0
    for subdir in parquet raw extracted; do
        if ! remote "test -d '$REMOTE_CACHE_DIR/$subdir'"; then
            log "WARNING: expected subdirectory missing after extract: $subdir/"
            missing=1
        fi
    done
    if [[ "$missing" -eq 0 ]]; then
        log "Extraction OK — parquet/, raw/, extracted/ present in $REMOTE_CACHE_DIR."
    fi
    log "Next: restart the app and check /api/health (frontend) and /admin/api/health (backend)."
}

# ── Main ────────────────────────────────────────────────────────────────
if [[ -z "$TARBALL" ]]; then
    TARBALL=$(ls -1t pspcz-cache-*.tar.gz 2>/dev/null | head -1 || true)
    [[ -n "$TARBALL" ]] || die "No pspcz-cache-*.tar.gz in the current directory — pass the archive path explicitly"
    log "Using newest archive: $TARBALL"
fi

log "upload_cache.sh starting (log: $LOG_FILE)"
preflight
transfer
verify
extract
log "Done."
