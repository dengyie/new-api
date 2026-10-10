#!/usr/bin/env bash
#
# apihub zero-downtime deploy.
#
# supervisord cannot do "start the new process before stopping the old one":
# `supervisorctl restart` tears the listener down first, and every request that
# arrives in that window is refused. This script inverts the order using
# SO_REUSEPORT: the incoming process binds the same port while the outgoing one
# is still serving, so the kernel always has at least one socket accepting.
#
# Two supervisor programs, `apihub-blue` and `apihub-green`, both point at the
# same binary. Exactly one is RUNNING; the other is STOPPED. A deploy starts the
# idle slot, waits until it is genuinely in the socket group, and only then
# drains the slot that was serving.
#
# The frontend is a SECOND, separately published artifact. `--web <tarball>`
# unpacks it into a per-version directory and binds that directory to the
# incoming slot via APIHUB_STATIC_DIR. It is per-slot on purpose: the handover
# is process-scoped, so two processes sharing one directory would serve a mix
# of old index.html and new script bundles for the seconds the overlap lasts.
# Without `--web` the binary serves its own embedded copy, which is exactly the
# pre-existing behaviour.
#
# IMPORTANT -- supervisord restarts any program whose config file changed when
# `supervisorctl update` runs. Rewriting the config of the slot that is
# currently serving would therefore restart it and drop in-flight requests. So
# this script only ever writes the config of a slot that is NOT running: the
# incoming slot is written before it starts, and the retired slot is rewritten
# afterwards, once it has stopped. Both end up identical again, ready for the
# next deploy.
#
# Usage:
#   deploy.sh --binary <path/to/new-api> --version <expected-version> [--web <dist.tar.gz>]
#   deploy.sh --bootstrap --binary <path> --version <v> [--web <dist.tar.gz>]
#   deploy.sh --status
#   deploy.sh --rollback
#
# Omit --web to deploy the binary alone; the frontend then comes from the copy
# embedded in it, which is the safe default and the instant way to undo a
# frontend release.

set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Configuration. Override via environment.
# ---------------------------------------------------------------------------
# The live deployment root. /data, not the old /tmp/mnt: that filesystem is a
# 30G volume that had reached 82% full, while /data is the root overlay with 79G
# free. NFS is deliberately not an option -- it is mounted vers=3,nolock, so
# SQLite and .git have to stay on local disk or the database corrupts.
DEFAULT_APP_ROOT="/data/new-api"
APP_ROOT="${APP_ROOT:-$DEFAULT_APP_ROOT}"
BIN_DIR="${BIN_DIR:-$APP_ROOT/bin}"
# The SQLite file. Derived from APP_ROOT so a deployment root owns its own
# database, and overridable so the path can be pinned somewhere else for a
# single deploy. That override is what makes a storage migration possible at
# all: the handover runs the outgoing and incoming slots at the same time, and
# two processes on two different database files would each keep their own copy
# of the quota ledger. Sharing one file is what keeps the handover lossless;
# SQLITE_PATH is therefore owned here and never inherited (see
# source_environment) rather than copied forward from whatever the last deploy
# happened to write.
DB_PATH="${DB_PATH:-$APP_ROOT/data/new-api.db}"
# Where loadbalancer.yaml and the database live. Kept separate from DB_PATH
# because the two are not moved together: a migration can repin the database
# while the editable policy file stays put, and vice versa.
DATA_DIR="${DATA_DIR:-$APP_ROOT/data}"
WEB_ROOT="${WEB_ROOT:-$APP_ROOT/web}"
WEB_LINK="${WEB_LINK:-$WEB_ROOT/current}"
WEB_KEEP="${WEB_KEEP:-3}"
# How many deploy backups to keep in $BACKUP_DIR. Each one carries a copy of
# the binary and a database snapshot, so the default costs roughly 350MB a
# slot -- 1.75GB at five. See prune_backup_dirs for what is and is not a
# candidate.
BACKUP_KEEP="${BACKUP_KEEP:-5}"
# Whether SUPERVISOR_CONF_DIR was named by the caller rather than defaulted.
# Captured BEFORE the assignment above -- after it, the variable is set on every
# path and the test would always answer "yes".
#
# guard_conf_dir has to ask this question, because the value alone cannot answer
# it: a deliberate root move legitimately keeps writing to /etc/supervisor/conf.d
# -- that is where the live slot configs are -- so comparing the value would
# refuse every real relocation while passing a rehearsal that happened to set it
# to the same string. Intent is the thing being checked, so record intent.
SUPERVISOR_CONF_DIR_EXPLICIT="${SUPERVISOR_CONF_DIR+x}"
SUPERVISOR_CONF_DIR="${SUPERVISOR_CONF_DIR:-/etc/supervisor/conf.d}"
BACKUP_DIR="${BACKUP_DIR:-$APP_ROOT/backups}"
LOG_DIR="${LOG_DIR:-$APP_ROOT/logs}"
# Where this script lives, and therefore where CI puts the slot guard it ships
# alongside. Both are uploaded to the same directory, so "next to me" is the
# right answer and needs no configuration.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WATCHDOG_SRC="${WATCHDOG_SRC:-$SCRIPT_DIR/slot-watchdog.sh}"
# A deployment root owns its guard, not the repository: the installed copy is
# what supervisord runs, and it is refreshed on every deploy so the running
# guard can never be an older version than the one under review.
GUARD_PATH="${GUARD_PATH:-$APP_ROOT/slot-watchdog.sh}"
STATE_FILE="${STATE_FILE:-$APP_ROOT/.apihub-deploy-state}"
LOCK_FILE="${LOCK_FILE:-$APP_ROOT/.apihub-deploy.lock}"
PORT="${PORT:-3000}"
READY_TIMEOUT="${READY_TIMEOUT:-90}"
FINGERPRINT_TIMEOUT="${FINGERPRINT_TIMEOUT:-20}"
SLOTS=(apihub-blue apihub-green)

# Completeness thresholds for a frontend bundle. These must stay equal to
# common.MinIndexBytes / common.MinStaticFiles on the Go side, the Makefile's
# verify-embed target, and the same-named step in build-release.yml. A bundle
# that fails here is exactly the 2026-10-03 blank-page shape: it builds, serves
# 200, and renders nothing.
MIN_WEB_INDEX_BYTES="${MIN_WEB_INDEX_BYTES:-200}"
MIN_WEB_STATIC_FILES="${MIN_WEB_STATIC_FILES:-10}"

EXPECTED_VERSION=""
NEW_BINARY=""
NEW_WEB=""
BOOTSTRAP=0
MODE="deploy"
BACKUP_PATH=""
ACTIVE_SLOT=""
NEW_SLOT=""
NEW_WEB_DIR=""
STAGED_WEB_DIR=""

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
log()  { printf '[%s] %s\n' "$(date '+%F %T')" "$*"; }
warn() { printf '[%s] WARN: %s\n' "$(date '+%F %T')" "$*" >&2; }
die()  { printf '[%s] FATAL: %s\n' "$(date '+%F %T')" "$*" >&2; exit 1; }

# Validate them here rather than trusting them into arithmetic later. In bash an
# unquoted non-numeric operand inside (( )) is read as an unset VARIABLE name
# and evaluates to 0 -- so BACKUP_KEEP="5x" printed an arithmetic complaint to
# stderr and then compared as "1 <= 0", which is false, which deleted every
# deploy backup except the rollback target. The complaint is not a failure: the
# function carried on and pruned. A one-character typo must stop the deploy, not
# silently empty the rollback history.
#
# Every one of these is consumed the same way, so every one of them gets the
# same guard:
#   WEB_KEEP                (( n <= WEB_KEEP ))             prunes web version dirs
#   BACKUP_KEEP             (( n <= BACKUP_KEEP ))          prunes deploy backups
#   READY_TIMEOUT           (( now + READY_TIMEOUT ))       handoff readiness deadline
#   FINGERPRINT_TIMEOUT     (( now + FINGERPRINT_TIMEOUT )) rollback fingerprint deadline
#   MIN_WEB_INDEX_BYTES     (( bytes < MIN_WEB_INDEX_BYTES ))  web package check
#   MIN_WEB_STATIC_FILES    (( count < MIN_WEB_STATIC_FILES )) web package check
# A malformed value in any of them is either a silent prune-everything or a check
# that never trips, and both are worse than refusing to deploy.
#
# Note this deliberately tests each variable by name rather than asserting the
# value is non-empty. ${VAR:-default} substitutes on empty AND unset, so an
# operator who exports an empty variable gets the default, not a failure here.
for _numeric in WEB_KEEP BACKUP_KEEP READY_TIMEOUT FINGERPRINT_TIMEOUT \
                MIN_WEB_INDEX_BYTES MIN_WEB_STATIC_FILES; do
  [[ ${!_numeric} =~ ^[0-9]+$ ]] ||
    die "$_numeric must be a non-negative integer, got '${!_numeric}'"
done
unset _numeric

# PORT never reaches (( )), but it is formatted with printf '%04X' to build the
# listening-socket probe and then interpolated into the slot's environment line.
# PORT=99999 renders as 1869F and PORT=abc makes printf fail; either way the
# reuseport check inspects the wrong socket while reporting success.
[[ $PORT =~ ^[0-9]+$ ]] || die "PORT must be a non-negative integer, got '$PORT'"
(( PORT >= 1 && PORT <= 65535 )) ||
  die "PORT must be between 1 and 65535, got '$PORT'"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ ${DEPLOY_SH_LIB:-0} != 1 && $# -gt 0 ]]; do
  case "$1" in
    --binary)    NEW_BINARY="${2:-}"; shift 2 ;;
    --version)   EXPECTED_VERSION="${2:-}"; shift 2 ;;
    --web)       NEW_WEB="${2:-}"; shift 2 ;;
    --bootstrap) BOOTSTRAP=1; shift ;;
    --status)    MODE="status"; shift ;;
    --rollback)  MODE="rollback"; shift ;;
    -h|--help)   sed -n '2,36p' "$0"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

if [[ ${DEPLOY_SH_LIB:-0} != 1 && $MODE == deploy ]]; then
  [[ -n $NEW_BINARY ]] || die "--binary is required"
  [[ -f $NEW_BINARY ]] || die "binary not found: $NEW_BINARY"
  [[ -n $EXPECTED_VERSION ]] || die "--version is required"
  [[ -z $NEW_WEB ]] || [[ -f $NEW_WEB ]] || die "web bundle not found: $NEW_WEB"
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
need_root() { [[ $(id -u) -eq 0 ]] || die "must run as root (supervisord and $APP_ROOT are root-owned)"; }
need_cmd()  { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

# verify_binary_version <path> <expected>
#
# Ask the artifact itself what it is, before anything on the host is touched.
#
# The runtime version reported by /api/status is NOT sufficient for this, because
# common.InitEnv overwrites common.Version from the VERSION environment variable
# -- which this script writes from --version itself. A binary whose build-time
# stamp silently failed to apply would still report the expected version once
# running, so checking the running process cannot distinguish "correctly stamped"
# from "stamp never happened".
#
# `--version` prints and exits in InitEnv, long before InitDB, so running it here
# opens no database, binds no port and touches no configuration. It does require
# the binary to be executable on this host, which is why a mismatch is reported as
# a refusal to guess rather than silently skipped.
verify_binary_version() {
  local path="$1" expected="$2" got
  [[ -x $path ]] || chmod +x "$path" 2>/dev/null || true
  got="$("$path" --version 2>/dev/null | tr -d '[:space:]')"
  if [[ $got != "$expected" ]]; then
    die "the binary at $path reports version '$got', but this deploy was told to
       expect '$expected'. Refusing to install it.

       This is what a build whose -X target did not resolve looks like: the linker
       applies no -X value to an unknown symbol and reports success, leaving
       common.Version at its compiled-in default. The build steps assert the stamp
       for exactly this reason."
  fi
  log "binary self-reports version $got, as expected"
}

# A rehearsal against a sandbox APP_ROOT must not silently keep writing into the
# real /etc/supervisor/conf.d. SUPERVISOR_CONF_DIR does NOT follow APP_ROOT, so
# overriding one without the other points every write_slot_conf at the live
# production configs while the script backs up and probes a sandbox. That is not
# hypothetical: a rehearsal of this script once did exactly that, and the
# production slot configs had to be restored from the backup it had taken.
#
# The test is whether SUPERVISOR_CONF_DIR was NAMED, not what it was set to. A
# deliberate relocation of APP_ROOT is supposed to keep writing to
# /etc/supervisor/conf.d -- the live apihub-blue/green configs live there, and
# moving the root is not a reason to stop managing them. Comparing values refused
# every real relocation while passing a rehearsal that happened to spell the
# same string. Naming the directory is the assertion that the caller knows where
# the slot configs are, which is the whole risk.
guard_conf_dir() {
  if [[ $APP_ROOT != "$DEFAULT_APP_ROOT" && -z $SUPERVISOR_CONF_DIR_EXPLICIT ]]; then
    die "APP_ROOT is '$APP_ROOT' but SUPERVISOR_CONF_DIR was not named (it defaults to $SUPERVISOR_CONF_DIR).
       The two are independent, so this run would overwrite the production
       apihub-blue/green configs. Set SUPERVISOR_CONF_DIR explicitly if the
       override really is intended."
  fi
}

sup() { supervisorctl "$@"; }

slot_state() { sup status "$1" 2>/dev/null | awk '{print $2}'; }

slot_pid() { sup status "$1" 2>/dev/null | sed -n 's/.*pid \([0-9][0-9]*\).*/\1/p'; }

running_slot() {
  local slot
  for slot in "${SLOTS[@]}"; do
    [[ $(slot_state "$slot") == "RUNNING" ]] && { echo "$slot"; return 0; }
  done
  return 1
}

idle_slot() {
  local slot
  for slot in "${SLOTS[@]}"; do
    [[ $slot != "$1" ]] && { echo "$slot"; return 0; }
  done
  die "could not determine the idle slot (active='$1')"
}

# The legacy single-process program that this script supersedes.
LEGACY_PROGRAM="new-api"
legacy_running() { [[ $(slot_state "$LEGACY_PROGRAM") == "RUNNING" ]]; }

# A process that came up healthy on /api/status might still have bound without
# SO_REUSEPORT. The kernel is the authority here: the new pid must show up as a
# listener on the port, which only happens inside a reuseport group.
#
# Teaches: ss is absent on some hosts and netstat on others, so try both. If
# neither can report socket ownership the check fails closed, because a
# readiness check that cannot fail is worse than no readiness check at all.
reuseport_group_ok() {
  local pid="$1" port_hex
  port_hex="$(printf '%04X' "$PORT")"

  if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null | grep -q "pid=$pid," && return 0
    return 1
  fi

  if command -v netstat >/dev/null 2>&1; then
    # netstat prints the port in decimal, and the owning process as "pid/name".
    netstat -ltnp 2>/dev/null | awk -v p="$pid" -v pp=":$PORT" '
      $4 ~ pp"$" && $NF ~ ("^" p "/") { found = 1 }
      END { exit !found }' && return 0
    return 1
  fi

  # Neither tool available: read the socket table directly and match the inode
  # against the process's open file descriptors. Here the port is hexadecimal and
  # the state column ($4) must be 0A for LISTEN -- that filter also excludes the
  # ESTABLISHED rows that share the same local port.
  local inode
  inode="$(awk -v ph=":$port_hex" 'NR>1 && $2 ~ ph"$" && $4=="0A" {print $10; exit}' \
    /proc/net/tcp /proc/net/tcp6 2>/dev/null)"
  [[ -n $inode ]] || return 1
  ls -l /proc/"$pid"/fd 2>/dev/null | grep -q "socket:\[$inode\]"
}

check_reuseport_logged() {
  local slot="$1"
  if grep -qs 'SO_REUSEPORT enabled' "$LOG_DIR/$slot.out.log" "$LOG_DIR/$slot.err.log"; then
    log "$slot: SO_REUSEPORT confirmed in the process log"
    return 0
  fi
  warn "$slot: no SO_REUSEPORT log line -- this build may predate reuseport support"
  return 1
}

# ---------------------------------------------------------------------------
# Frontend bundle
#
# The bundle is validated BEFORE it is bound to a slot, so a bad one can never
# reach a running process. The check is the same pair of thresholds the binary
# applies to its embedded copy: an index.html that is a plausible size and a
# static tree with a plausible number of files. Both are cheap, and both catch
# the failure that actually happened in production on 2026-10-03 -- a build
# that produced an index.html but almost no assets.
# ---------------------------------------------------------------------------

# static_file_count <dir> -- regular files under <dir>/static; directories do
# not count, matching common.LoadStaticBundle on the Go side.
static_file_count() {
  [[ -d "$1/static" ]] || { echo 0; return 0; }
  find "$1/static" -type f 2>/dev/null | wc -l | tr -d ' '
}

file_size() {
  # GNU stat on tebi, BSD stat on a developer machine; the test suite runs on
  # both, and a silently-wrong size here would defeat the bundle check.
  stat -c %s "$1" 2>/dev/null || stat -f %z "$1" 2>/dev/null || echo 0
}

file_mtime() {
  # Same GNU/BSD split as file_size. `stat -c '%y'` alone would print nothing at
  # all on macOS, so a status report would look like the file does not exist.
  stat -c '%y' "$1" 2>/dev/null || stat -f '%Sm' "$1" 2>/dev/null || echo unknown
}

# served_version <slot> -- what /api/status says the running slot is serving.
# Reported by --status because "which binary is actually live" is otherwise only
# answerable by reading the supervisor config and trusting it.
served_version() {
  local body
  body="$(http_get /api/status)"
  [[ -n $body ]] || { echo '<not answering>'; return 0; }
  sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' <<<"$body" | head -1
}

sha256_short() {
  # sha256sum on tebi, shasum on macOS.
  sha256sum "$1" 2>/dev/null | cut -c1-16 || shasum -a 256 "$1" 2>/dev/null | cut -c1-16 || echo unknown
}

verify_web_bundle() {
  local dir="$1"
  local index="$dir/index.html" bytes count
  [[ -d $dir ]] || die "frontend bundle directory does not exist: $dir"
  [[ -f $index ]] || die "frontend bundle has no index.html: $index"

  bytes="$(file_size "$index")"
  if (( bytes < MIN_WEB_INDEX_BYTES )); then
    die "frontend index.html is only $bytes bytes (need >= $MIN_WEB_INDEX_BYTES); refusing to publish a bundle that would render a blank page"
  fi

  count="$(static_file_count "$dir")"
  if (( count < MIN_WEB_STATIC_FILES )); then
    die "frontend static tree has only $count files (need >= $MIN_WEB_STATIC_FILES); refusing to publish"
  fi

  log "frontend bundle verified: index.html ${bytes}B, $count static files"
}

# web_fingerprint <dir> -- the asset URLs index.html pulls in. Two builds of
# different code produce different content-hashed filenames, so these strings
# are what distinguishes "serving the frontend we just shipped" from "serving
# something older".
web_fingerprint() {
  grep -oE '(src|href)="[^"]+\.(js|css)"' "$1/index.html" 2>/dev/null \
    | sed 's/^[^"]*"//; s/"$//' | sort -u
}

# The directory a slot's supervisor config points at, read back from the config
# rather than tracked separately, so the two can never disagree.
slot_static_dir() {
  local conf="$SUPERVISOR_CONF_DIR/$1.conf" v
  [[ -f $conf ]] || return 0
  v="$(sed -n 's/.*APIHUB_STATIC_DIR="\([^"]*\)".*/\1/p' "$conf" | head -1)"
  # Accept whatever real bundle directory the config names, not only one under
  # the current WEB_ROOT. During a root migration the live slot is still bound to
  # the *previous* root -- that is the whole point of the per-slot binding -- so
  # testing against WEB_ROOT discarded a perfectly good directory and recorded
  # an empty value, which a later --rollback would read as "served the embedded
  # copy" and silently give up the frontend binding.
  #
  # "Is a directory" would be too loose in the other direction: the configs are
  # ordinary files on disk and anything hand-edited into one, /etc among them,
  # is a directory. What distinguishes a frontend bundle is index.html, so test
  # for that; verify_web_bundle checks the thresholds before anything serves it.
  if [[ -z $v || ! -f $v/index.html ]]; then v=""; fi
  printf '%s' "$v"
}

# stage_web_bundle <tarball> -- unpack into a per-version directory, verify it,
# and set STAGED_WEB_DIR. The result is a global rather than stdout because the
# logging helpers also write to stdout and would end up in a command
# substitution.
#
# Extracting into a fresh version directory rather than over a shared "current"
# path is the whole point: the outgoing process keeps reading the directory it
# was bound to for as long as it lives, so neither side can ever see the other
# build's files.
stage_web_bundle() {
  local tarball="$1"
  need_cmd tar

  # A tarball from CI is trusted input, but an absolute or parent-relative member
  # would unpack outside WEB_ROOT, so refuse the archive rather than the damage.
  if tar -tzf "$tarball" | grep -qE '^/|(^|/)\.\.(/|$)'; then
    die "web bundle contains an absolute or parent-relative path; refusing to unpack"
  fi

  local slug="${EXPECTED_VERSION//[^A-Za-z0-9._-]/_}"
  local dir="$WEB_ROOT/$slug"
  local active_dir=""
  [[ -n $ACTIVE_SLOT ]] && active_dir="$(slot_static_dir "$ACTIVE_SLOT")"

  if [[ -d $dir && $dir == "$active_dir" ]]; then
    # Re-deploying a version that is currently being served: replacing the files
    # under a live process is exactly the mismatch per-slot directories exist to
    # prevent. Take a sibling directory instead.
    dir="$WEB_ROOT/$slug-r$(date +%s)"
    log "web directory for $slug is in use by $ACTIVE_SLOT; staging this build as $(basename "$dir")"
  fi

  local staging="$WEB_ROOT/.staging.$$"
  rm -rf "$staging"
  mkdir -p "$staging"
  tar -xzf "$tarball" -C "$staging"

  # Some tarballs carry a single top-level directory; unwrap it so the shape
  # matches what the Go loader expects (index.html at the root).
  if [[ ! -f "$staging/index.html" && -d "$staging"/*/ ]]; then
    local inner
    # -print -quit rather than `| head -1`: head closes the pipe on the first
    # line, find dies of SIGPIPE, and under `set -o pipefail` that surfaces as a
    # deploy aborting with "web bundle has no index.html at its root" for a
    # bundle that had one all along.
    inner="$(find "$staging" -mindepth 1 -maxdepth 1 -type d -print -quit)"
    [[ -n $inner && -f "$inner/index.html" ]] || { rm -rf "$staging"; die "web bundle has no index.html at its root"; }
    local flat="$staging.flat"
    mv "$inner" "$flat"
    rm -rf "$staging"
    mv "$flat" "$staging"
  fi

  verify_web_bundle "$staging"

  rm -rf "$dir"
  mv "$staging" "$dir"
  log "frontend staged at $dir ($(du -sh "$dir" | cut -f1), tarball sha256 $(sha256_short "$tarball"))"
  STAGED_WEB_DIR="$dir"
}

# The `current` symlink is for humans only. Nothing serves through it -- every
# slot is bound to a concrete directory -- so a stale or dangling link can
# never break a request, and an empty argument means "embedded in the binary"
# and removes the link rather than pointing it at nothing.
publish_web_link() {
  local dir="${1:-}"
  if [[ -z $dir ]]; then
    rm -f "$WEB_LINK"
  else
    ln -sfn "$dir" "$WEB_LINK"
  fi
}

# main.go calls loadbalancer.Init("data/loadbalancer.yaml") -- a path relative
# to the working directory, which supervisord sets to BIN_DIR. Nothing in the
# Go code resolves it against the executable, the database, or an environment
# variable, so the only reason production reads its configured routing policy
# instead of the compiled-in default is a `data` symlink inside BIN_DIR.
# Relocate the deployment root without recreating that link and the app starts
# perfectly cleanly against default load balancing: no error, no log line, just
# different routing decisions. Recreate it here rather than leaving it to the
# operator.
ensure_data_link() {
  local link="$BIN_DIR/data"
  [[ -d $DATA_DIR ]] || return 0
  if [[ -L $link ]]; then
    [[ $(readlink "$link") == "$DATA_DIR" ]] && return 0
  elif [[ -e $link ]]; then
    die "$link exists and is not a symlink; refusing to replace it"
  fi
  ln -sfn "$DATA_DIR" "$link"
  log "linked $link -> $DATA_DIR (this is how data/loadbalancer.yaml is found)"
}

# The slot guard is a supervisord program, installed and registered here rather
# than armed by hand. The hand-armed version was correct code that nothing kept
# alive: it was gone after the next reboot while the runbook described it as a
# standing protection, which is a worse failure than having no guard at all,
# because it reads as covered. Installing it on every deploy means there is no
# step to forget and no way for the running copy to drift from the reviewed one.
install_slot_guard() {
  local conf="$SUPERVISOR_CONF_DIR/apihub-slot-guard.conf"

  # A manual run of deploy.sh from an old checkout has no guard to upload.
  # Skip rather than fail: this adds a protection, it is not a precondition for
  # the deploy, and refusing to deploy because a spare file is absent would be
  # trading a real capability for a cosmetic one.
  if [[ ! -f $WATCHDOG_SRC ]]; then
    warn "no slot guard at $WATCHDOG_SRC; leaving the installed guard as it is"
    return 0
  fi

  if [[ "$WATCHDOG_SRC" != "$GUARD_PATH" ]]; then
    install -m 0755 "$WATCHDOG_SRC" "$GUARD_PATH"
  else
    chmod 0755 "$GUARD_PATH"
  fi

  # The script's digest goes in a comment on purpose. supervisorctl update only
  # restarts a program whose CONFIG changed, so without it a fixed guard would
  # keep running the old code until something else happened to rewrite the file.
  local tmp="$conf.new"
  cat >"$tmp" <<EOF
; Generated by scripts/deploy.sh -- do not edit by hand.
; Starts one APIHub slot if neither is RUNNING. It never writes a config and
; never stops anything; see scripts/slot-watchdog.sh for why it exists.
; script sha256: $(sha256_short "$WATCHDOG_SRC")
[program:apihub-slot-guard]
command=$GUARD_PATH 2 ${GUARD_TRIGGER:-60}
directory=$BIN_DIR
user=root
autostart=true
autorestart=true
; A guard that crashes three times is marked FATAL and left alone: visible, and
; not a restart loop. autorestart=true on a genuinely broken script would
; otherwise fill the log at supervisord's retry rate.
startretries=3
startsecs=10
stopsignal=TERM
stdout_logfile=$LOG_DIR/apihub-slot-guard.log
stdout_logfile_maxbytes=5MB
stdout_logfile_backups=3
stderr_logfile=$LOG_DIR/apihub-slot-guard.err.log
stderr_logfile_maxbytes=5MB
stderr_logfile_backups=3
EOF
  mv -f "$tmp" "$conf"
  log "installed the slot guard at $GUARD_PATH (trigger ${GUARD_TRIGGER:-60}s)"

  # Load it now rather than at the next apply_supervisor_conf, so the guard is
  # already watching before this deploy reaches anything it could break.
  apply_supervisor_conf || die "supervisord did not accept $conf; no slot is stopped yet, but the guard is not running"

  # supervisord reports a rejected program block on stderr and then simply does
  # not load it, while still updating the programs it did accept -- so a
  # successful `update` is not evidence that the guard exists. Ask directly.
  # A warning rather than a failure: the guard is a spare, and refusing to ship
  # a binary over a spare would trade a real capability for a cosmetic one. It
  # is loud because the whole point of this exercise is that an unstarted
  # safeguard is indistinguishable from a healthy one unless someone says so.
  case "$(slot_state apihub-slot-guard)" in
    RUNNING) log "slot guard is up" ;;
    *) warn "the slot guard is NOT running; check $LOG_DIR/apihub-slot-guard.err.log" ;;
  esac
}

# prune_web_dirs <protected...> -- keep the newest WEB_KEEP version directories
# plus anything named on the command line. The protected list always includes
# the directory the live slot serves and the one --rollback would return to, so
# pruning can never remove the only frontend a rollback could use.
prune_web_dirs() {
  [[ -d $WEB_ROOT ]] || return 0
  # Reduce the protected list to basenames before comparing. Both callers pass
  # absolute paths -- NEW_WEB_DIR and slot_static_dir are both built from
  # WEB_ROOT -- so testing them verbatim against a basename never matches, and
  # the guard silently protected nothing. That is not hypothetical: it deleted
  # the live slot's own bundle on the first real deployment, leaving the console
  # quietly serving the binary's embedded copy.
  local keep_list=" " p
  for p in "$@"; do
    [[ -n $p ]] && keep_list+="$(basename "$p") "
  done
  local d base n=0
  # Newest first. `ls -dt` rather than `find -printf`, which is GNU-only, so the
  # test suite runs on macOS as well as on tebi.
  while IFS= read -r d; do
    # ls prints directories with a trailing slash, and a trailing slash makes
    # [[ -L ]] follow the link instead of reporting it. Strip it first, then
    # require a real directory: `current` is a symlink to a directory, so
    # [[ -d ]] alone accepts it, spends a retention slot on it, and eventually
    # hands it to rm -rf.
    d="${d%/}"
    [[ -d $d && ! -L $d ]] || continue
    base="$(basename "$d")"
    [[ $base == .staging.* ]] && continue
    [[ $base == "$(basename "$WEB_LINK")" ]] && continue
    # Protected directories never count against the retention budget: the live
    # one and the one --rollback would return to must survive regardless of how
    # many deploys have happened since.
    [[ $keep_list == *" $base "* ]] && continue
    n=$((n + 1))
    (( n <= WEB_KEEP )) && continue
    rm -rf "$d"
    log "pruned old frontend directory $base"
  done < <(ls -1dt "$WEB_ROOT"/*/ 2>/dev/null)
  rm -rf "$WEB_ROOT"/.staging.* 2>/dev/null || true
}

# prune_backup_dirs <protected...> -- keep the newest BACKUP_KEEP deploy backups
# plus anything named on the command line.
#
# Why this exists: do_backup writes a fresh timestamped directory on every deploy,
# each carrying a copy of the binary (~130MB) and a database snapshot (~210MB).
# Nothing removed the old ones, so backups/ grew by ~350MB per deployment with no
# ceiling. After a day and a half it was 7.7GB -- the largest consumer on the root
# filesystem, and the thing that eventually fills the disk and takes the database
# down with it.
#
# Two guards, because this deletes the artifact --rollback restores from:
#
#   * Only a directory named exactly YYYYMMDD-HHMMSS is a candidate. The
#     hand-named ones (stale-db-snapshots, autorecover-*, pre-*, whitelist-*) are
#     deliberate safety copies, not deploy residue, and are never touched.
#   * The directory recorded in .apihub-deploy-state -- the one --rollback
#     consumes -- is protected, and does not spend a retention slot.
prune_backup_dirs() {
  [[ -d $BACKUP_DIR ]] || return 0
  local keep_list=" " p
  for p in "$@"; do
    [[ -n $p ]] && keep_list+="$(basename "$p") "
  done
  local d base n=0
  # Newest name first. Two deliberate choices here, and the second one is a trap I
  # walked into while writing it.
  #
  # Name order rather than mtime order: for a YYYYMMDD-HHMMSS name the two agree,
  # but mtime also moves when anything inside is copied with -a or restored, and a
  # retention policy that reorders itself after a restore is not one anybody can
  # reason about. The name is the stamp, and the stamp is the age.
  #
  # DESCENDING, because the counter below keeps the first BACKUP_KEEP entries it
  # sees. Ascending would have kept the oldest N and deleted the newest -- which
  # is the exact opposite of a retention policy, and would have thrown away the
  # most recent deploy backups while preserving ancient ones.
  while IFS= read -r d; do
    d="${d%/}"
    [[ -d $d && ! -L $d ]] || continue
    base="$(basename "$d")"
    [[ $base =~ ^[0-9]{8}-[0-9]{6}$ ]] || continue
    [[ $keep_list == *" $base "* ]] && continue
    n=$((n + 1))
    (( n <= BACKUP_KEEP )) && continue
    rm -rf "$d"
    log "pruned old deploy backup $base"
  done < <(ls -1dr "$BACKUP_DIR"/*/ 2>/dev/null)
}

# ---------------------------------------------------------------------------
# Supervisor configuration
#
# The environment line is copied verbatim from the existing production config so
# that tebi-specific values (SESSION_SECRET, proxy, SQLITE_PATH) survive the
# migration untouched. Nothing here rewrites those values -- only the program
# name, the log file, and two appended variables.
# ---------------------------------------------------------------------------
source_environment() {
  local conf="$SUPERVISOR_CONF_DIR/$LEGACY_PROGRAM.conf"
  if [[ ! -f $conf ]]; then
    # After the first handover the legacy config is gone; the retired slot's
    # config is the one that still carries the untouched production
    # environment, so read it from there instead.
    local s
    for s in "${SLOTS[@]}"; do
      conf="$SUPERVISOR_CONF_DIR/$s.conf"
      [[ -f $conf ]] && break
    done
  fi
  [[ -f $conf ]] || die "cannot find $LEGACY_PROGRAM.conf or any slot config; refusing to guess the production environment"

  local line
  line="$(grep -E '^environment=' "$conf" | head -1)"
  [[ -n $line ]] || die "$conf has no environment= line"
  [[ $line == *SESSION_SECRET* ]] || warn "$conf has no SESSION_SECRET; sessions will not survive the deploy"

  line="${line#environment=}"

  # Drop any APIHUB_STATIC_DIR inherited from the config being read, so a
  # rollback does not silently inherit the release it is rolling back from.
  #
  # This runs BEFORE the trailing quote is stripped, because that quote belongs
  # to the last element -- if the last element is the one being removed, taking
  # the quote first would leave the survivor without its terminator and produce
  # a doubled quote when the new pairs are appended. The last element has to go
  # through the loop too: it is the one most likely to hold the variable, since
  # deploy.sh always appends its own pairs after the original list.
  local pair out="" first=1
  while :; do
    if [[ $line == *,* ]]; then
      pair="${line%%,*}"; line="${line#*,}"
    else
      pair="$line"; line=""
    fi
    case "$pair" in
      APIHUB_STATIC_DIR=*|APIHUB_REUSEPORT=*|VERSION=*|SQLITE_PATH=*) ;;
      *)
        # Separators go BETWEEN pairs, never after the last one. Appending a
        # comma per pair and trimming it at the end leaves a quoted final value
        # looking like it lost its quote, which then got added back.
        if (( first )); then out="$pair"; first=0; else out="${out},${pair}"; fi ;;
    esac
    [[ -n $line ]] || break
  done
  line="$out"

  # `line` is now a verbatim copy of the original pairs, so extending it only
  # needs a comma. The old code stripped a trailing quote and re-opened one,
  # which is what turned a final PORT=3998 into PORT=3998".
  # A leading comma would be a syntax error, so an empty result stays empty.
  ENVIRONMENT_LINE="${line:+${line},}APIHUB_REUSEPORT=1,VERSION=\"${EXPECTED_VERSION}\",SQLITE_PATH=\"${DB_PATH}\""
}

# write_slot_conf <slot> <autostart:0|1> [static_dir]
#
# Only ever call this for a slot that is not currently RUNNING. static_dir is
# the frontend bundle bound to that slot; empty means "use the copy embedded in
# the binary", which is the pre-separation behaviour.
write_slot_conf() {
  local slot="$1" autostart="$2" static_dir="${3:-}" conf="$SUPERVISOR_CONF_DIR/$1.conf"

  local static_line=""
  [[ -n $static_dir ]] && static_line=",APIHUB_STATIC_DIR=\"${static_dir}\""

  # Write to a sibling and rename. supervisord reads these files on `reread`,
  # and a `cat > conf` that is interrupted -- or that races a reader -- leaves a
  # half-written config on disk, which supervisord then either rejects outright
  # or, worse, loads a truncated environment line. mv within a directory is
  # atomic, so a reader sees either the old file or the new one.
  local tmp="$conf.new"
  cat >"$tmp" <<EOF
; Generated by scripts/deploy.sh -- do not edit by hand.
; One of these two slots is live at any time; deploy.sh switches which.
; Both bind the same port with SO_REUSEPORT, so during a handover the kernel
; spreads new connections across both while each drains its own in-flight work.
; APIHUB_STATIC_DIR (when present) is this slot's own frontend bundle. It is
; per-slot, never shared: the outgoing process must keep serving the frontend it
; started with for as long as it lives.
[program:$slot]
directory=$BIN_DIR
command=$BIN_DIR/new-api --port $PORT
user=root
autostart=$autostart
autorestart=true
stopsignal=TERM
; Must exceed the application's own SHUTDOWN_TIMEOUT_SECONDS (default 120) so
; supervisord does not SIGKILL a process still draining long-lived SSE streams.
stopwaitsecs=150
stopasgroup=true
killasgroup=true
environment=$ENVIRONMENT_LINE$static_line
redirect_stderr=true
stdout_logfile=$LOG_DIR/$slot.out.log
stderr_logfile=$LOG_DIR/$slot.err.log
stdout_logfile_maxbytes=10MB
stdout_logfile_backups=5
stderr_logfile_maxbytes=10MB
stderr_logfile_backups=5
EOF
  mv -f "$tmp" "$conf"
  log "wrote $conf${static_dir:+ (frontend: $static_dir)}"
}

# apply_supervisor_conf -- reload. Safe only when no RUNNING slot's config
# changed, because supervisord restarts programs whose config it reloads.
apply_supervisor_conf() {
  need_cmd supervisorctl
  sup reread 2>&1 | sed 's/^/  reread: /' || true
  # `update` is the only thing that actually loads a rewritten config, and its
  # stdout ("<slot>: updated process group") is the only direct evidence that
  # the reload happened. Discarding both its output and its exit code made a
  # failed reload indistinguishable from "the config was already correct" --
  # and the check that would otherwise notice, the frontend fingerprint, does
  # not run until after the serving slot has already been retired. Callers
  # decide whether a non-zero answer is fatal, because it is: fatal before the
  # handover (the old slot is still serving), a warning after it.
  local out rc=0
  out="$(sup update 2>&1)" || rc=$?
  [[ -n $out ]] && printf '  update: %s\n' "$out"
  if (( rc != 0 )); then
    warn "supervisorctl update exited $rc; supervisord may still be running the previous configs"
    return 1
  fi
  local s
  for s in "${SLOTS[@]}"; do
    log "$s -> $(slot_state "$s" || echo UNKNOWN)$(slot_pid "$s" >/dev/null 2>&1 && echo " pid=$(slot_pid "$s")")"
  done
}

# ---------------------------------------------------------------------------
# Backup
#
# Taken before the binary is touched so a bad build can be rolled back in
# seconds without a rebuild. The database is copied through sqlite3's online
# backup API where available, since copying the file while it is being written
# can capture a torn page.
# ---------------------------------------------------------------------------
do_backup() {
  local stamp dest f
  stamp="$(date '+%Y%m%d-%H%M%S')"
  dest="$BACKUP_DIR/$stamp"
  mkdir -p "$dest"
  BACKUP_PATH="$dest"

  [[ -f $BIN_DIR/new-api ]] && cp -a "$BIN_DIR/new-api" "$dest/new-api.prev"

  for f in "$SUPERVISOR_CONF_DIR/$LEGACY_PROGRAM.conf" "$SUPERVISOR_CONF_DIR"/apihub-*.conf; do
    [[ -f $f ]] && cp -a "$f" "$dest/$(basename "$f")"
  done

  # Which frontend the live slot is bound to right now. Recorded here so a
  # rollback restores the binary and the frontend together; restoring one
  # without the other is how a deploy ends up serving a console whose scripts
  # the API no longer matches. Read from the config, so it cannot drift from
  # what the process actually has.
  local live_slot="" live_web=""
  live_slot="$(running_slot 2>/dev/null || true)"
  if [[ -n $live_slot ]]; then
    live_web="$(slot_static_dir "$live_slot")"
  fi
  printf '%s' "$live_web" >"$dest/deploy-state.webdir"
  log "recorded live frontend for $live_slot: ${live_web:-<embedded in binary>}"

  local db="$DB_PATH"
  if [[ -f $db ]]; then
    if command -v sqlite3 >/dev/null 2>&1; then
      sqlite3 "$db" ".backup '$dest/new-api.db'" && log "database snapshotted via sqlite3 .backup"
    else
      cp -a "$db" "$dest/new-api.db"
      warn "sqlite3 not found; took a plain database copy"
    fi
  fi

  # Timestamped config snapshot, per the standing rule that config changes are
  # backed up on the persistent volume before anything is modified.
  [[ -f "$APP_ROOT/config.yaml" ]] && cp -a "$APP_ROOT/config.yaml" "$dest/config.yaml.bak_$stamp"

  log "backup complete: $dest"
}

# ---------------------------------------------------------------------------
# Readiness
#
# Both slots share one port, so a 200 from /api/status proves nothing on its
# own -- the response may have come from the process being retired. Readiness
# therefore requires the new pid to be a member of the listening socket group
# AND to have answered at least two probes. Once the old slot has been drained
# the same check runs again against the survivor.
# ---------------------------------------------------------------------------
wait_ready() {
  local slot="$1" pid="$2" deadline body ok=0 attempts=0 served=""
  deadline=$(( $(date +%s) + READY_TIMEOUT ))

  while (( $(date +%s) < deadline )); do
    attempts=$((attempts + 1))

    if ! kill -0 "$pid" 2>/dev/null; then
      warn "$slot (pid $pid) exited during startup. Recent log lines:"
      tail -20 "$LOG_DIR/$slot.err.log" >&2 2>/dev/null || true
      tail -20 "$LOG_DIR/$slot.out.log" >&2 2>/dev/null || true
      return 1
    fi

    if reuseport_group_ok "$pid"; then
      body="$(http_get "/api/status")"
      if [[ -n $body ]]; then
        # Is this the build we were asked to install? /api/status reports
        # common.Version, so a stale artifact is detectable -- but only if
        # somebody looks. Everything else in this script can pass while the
        # wrong binary is serving: the frontend fingerprint would still match
        # if the bundle is current, and "answered two probes" only proves
        # something is listening. Without this check a deploy of the wrong
        # file reports success.
        served="$(sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' <<<"$body" | head -1)"
        if [[ -z $EXPECTED_VERSION || $served == "$EXPECTED_VERSION" ]]; then
          ok=$((ok + 1))
          if (( ok >= 2 )); then
            log "$slot (pid $pid) is in the socket group and answered $ok/$attempts probes on version ${served:-<unreported>}"
            return 0
          fi
        else
          warn "$slot reports version '$served' but this deploy was told to expect '$EXPECTED_VERSION'"
        fi
      fi
    fi
    sleep 1
  done

  warn "$slot did not become ready within ${READY_TIMEOUT}s (answered $ok of $attempts probes; last reported version: ${served:-<none>})"
  return 1
}

# ---------------------------------------------------------------------------
# Local HTTP probe
#
# Every probe in this script targets 127.0.0.1, but curl still honours the
# operator's ~/.curlrc and the ambient proxy environment. If a proxy is
# configured (tebi runs Clash on 127.0.0.1:7897 via ~/.curlrc), curl sends
# the request to the proxy instead of the socket, and a proxy that cannot
# reach loopback answers 502 -- which reads here as "not ready yet" and can
# fail a perfectly healthy handover. `-q` disables .curlrc and `--noproxy`
# disables the environment, so a probe can only ever hit the local socket.
# ---------------------------------------------------------------------------
http_get() {
  curl -q --noproxy '*' -fsS --max-time 5 "http://127.0.0.1:$PORT$1" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Frontend verification, after the handover
#
# Readiness proves the process is alive and answering. It does not prove it is
# answering with the frontend this deploy just installed -- the config could
# have failed to pick up APIHUB_STATIC_DIR, the directory could have been
# unreadable, or the request could have landed on the retired slot. All three
# produce a perfectly healthy service showing a stale console, which is silent.
#
# This runs only after the old slot has drained, so the port has exactly one
# owner and the answer is unambiguous.
# ---------------------------------------------------------------------------
verify_frontend_served() {
  local slot="$1" dir="$2" deadline body tokens missing token
  deadline=$(( $(date +%s) + FINGERPRINT_TIMEOUT ))

  if [[ -z $dir ]]; then
    # No bundle deployed: the binary serves its embedded copy. There is no
    # fingerprint to match, but the page still has to be a real document --
    # a 200 with an empty body is precisely the blank-screen failure.
    while (( $(date +%s) < deadline )); do
      body="$(http_get /)"
      if [[ -n $body ]] && (( ${#body} >= MIN_WEB_INDEX_BYTES )); then
        log "$slot: serving the embedded frontend (${#body}B index page)"
        return 0
      fi
      sleep 1
    done
    warn "$slot did not serve a usable index page within ${FINGERPRINT_TIMEOUT}s"
    return 1
  fi

  tokens="$(web_fingerprint "$dir")"
  [[ -n $tokens ]] || { warn "$dir/index.html references no .js/.css assets; cannot fingerprint it"; return 1; }

  missing=""
  while (( $(date +%s) < deadline )); do
    body="$(http_get /)"
    if [[ -n $body ]]; then
      missing=""
      while IFS= read -r token; do
        [[ -z $token ]] && continue
        grep -qF -- "$token" <<<"$body" || missing="${missing} ${token}"
      done <<<"$tokens"
      [[ -z $missing ]] && { log "$slot: serving the frontend from $dir (fingerprint matched)"; return 0; }
    fi
    sleep 1
  done

  warn "$slot is not serving the frontend that was just installed; missing from the served page:$missing"
  return 1
}

# A fingerprint miss means the console users are looking at is not the one this
# deploy shipped. Rolling back automatically is the right default here: the
# handover is already complete and there is no old process left to fall back to
# gradually, and a half-applied frontend release is worse than the previous
# known-good one.
recover_from_frontend_miss() {
  printf '%s' "$BACKUP_PATH" >"$STATE_FILE"
  warn "rolling back automatically to the previous release"
  do_rollback || die "automatic rollback failed -- start a slot by hand. Backup: $BACKUP_PATH"
  die "the frontend did not match the deployed bundle; the previous release has been restored"
}

# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------
do_rollback() {
  need_root
  [[ -f $STATE_FILE ]] || die "no $STATE_FILE; nothing to roll back to"

  local backup prev_slot prev_version prev_web slot autostart
  backup="$(cat "$STATE_FILE")"
  [[ -d $backup ]] || die "recorded backup $backup no longer exists"
  log "rolling back to $backup"

  # ---------------------------------------------------------------------
  # Preparation. Everything between here and the marker is read-only apart from
  # one chmod, and nothing has been stopped yet. That ordering is the whole
  # point of this function: it is the only path that stops BOTH slots at once,
  # so a refusal that arrives after the stop loop costs the site, while the same
  # refusal before it costs a message. That is the 2026-10-09 outage shape --
  # the script that takes the service down being the only thing responsible for
  # bringing it back -- and it is not a shape worth writing twice.
  # ---------------------------------------------------------------------
  [[ -f $backup/new-api.prev ]] || die "backup $backup has no previous binary"

  # The version comes from the artifact about to be restored, NOT from
  # deploy-state.version. That file records the version this deploy moved TO,
  # and VERSION is a runtime environment override rather than a build stamp --
  # common.InitEnv overwrites common.Version with it, so an old binary launched
  # with the wrong string reports the wrong string, and wait_ready's version
  # check degenerates into comparing a value with itself. Asking the binary is
  # the discipline verify_binary_version already applies on the deploy side, and
  # it has the useful side effect of making a backup that carries no
  # deploy-state.version at all usable -- which is what a --bootstrap deploy
  # leaves behind.
  [[ -x $backup/new-api.prev ]] || chmod +x "$backup/new-api.prev" 2>/dev/null || true
  prev_version="$("$backup/new-api.prev" --version 2>/dev/null | tr -d '[:space:]')"
  [[ -n $prev_version ]] || die "$backup/new-api.prev does not report a version; refusing to restore an artifact that cannot be identified"

  prev_slot="$(cat "$backup/deploy-state.slot" 2>/dev/null || echo apihub-blue)"
  prev_slot="${prev_slot//[$'\n\r']/}"

  # Restore the frontend that was live alongside that binary. A backup recorded
  # before the frontend was separated records an empty value, which is correct:
  # it means "served the copy embedded in the binary".
  prev_web="$(cat "$backup/deploy-state.webdir" 2>/dev/null || true)"
  prev_web="${prev_web//[$'\n\r']/}"
  if [[ -n $prev_web && ! -d $prev_web ]]; then
    warn "the recorded frontend directory $prev_web is gone; falling back to the embedded copy"
    prev_web=""
  elif [[ -n $prev_web ]]; then
    verify_web_bundle "$prev_web" || die "the recorded frontend directory is no longer usable: $prev_web"
  fi
  # ------------------------- end of preparation -------------------------

  # Rollback is one-way unless the outgoing binary is kept. It survives nowhere
  # else: the backup holds the version being rolled back TO, and CI deletes the
  # uploaded release immediately afterwards. Without this, a rollback that lands
  # on a second bad build has nothing left to return to -- which leaves "just
  # roll it back" as advice with no second step.
  if [[ -f $BIN_DIR/new-api ]]; then
    cp -a "$BIN_DIR/new-api" "$backup/new-api.rolled-back-from"
    log "kept the outgoing binary as new-api.rolled-back-from"
  fi

  for slot in "${SLOTS[@]}"; do sup stop "$slot" >/dev/null 2>&1 || true; done

  cp -a "$backup/new-api.prev" "$BIN_DIR/new-api"
  chmod +x "$BIN_DIR/new-api"
  EXPECTED_VERSION="$prev_version"
  log "restored the previous binary ($prev_version)"

  # Both slots are stopped now, so rewriting both configs is safe.
  source_environment
  for slot in "${SLOTS[@]}"; do
    autostart=0
    [[ $slot == "$prev_slot" ]] && autostart=1
    write_slot_conf "$slot" "$autostart" "$prev_web"
  done
  apply_supervisor_conf

  sup start "$prev_slot" || die "failed to restart $prev_slot"
  log "$prev_slot restarted on version $EXPECTED_VERSION (frontend: ${prev_web:-<embedded>}); verifying"
  wait_ready "$prev_slot" "$(slot_pid "$prev_slot")" || die "rollback did not come up cleanly; inspect $LOG_DIR/$prev_slot.err.log"
  verify_frontend_served "$prev_slot" "$prev_web" || warn "the rolled-back slot did not serve the expected frontend; check the APIHUB_STATIC_DIR setting"
  publish_web_link "$prev_web" 2>/dev/null || true
  log "rollback complete"
}

show_status() {
  log "supervisor slots:"
  sup status "${SLOTS[@]}" 2>/dev/null || true
  log ""
  # supervisorctl answers "ERROR (no such process)" and exits 4 for a program it
  # has never heard of, which is the normal state once the legacy program has
  # been retired. Printing that ERROR verbatim makes a healthy host look broken,
  # and --status is the first command anyone runs when it is not.
  local legacy_state
  legacy_state="$(slot_state "$LEGACY_PROGRAM" 2>/dev/null || true)"
  case $legacy_state in
    ""|ERROR*) log "legacy program '$LEGACY_PROGRAM': not registered with supervisord" ;;
    *)         log "legacy program '$LEGACY_PROGRAM': $legacy_state" ;;
  esac
  log "live binary: $(file_mtime "$BIN_DIR/new-api" 2>/dev/null || echo 'not found'), $(file_size "$BIN_DIR/new-api") bytes"
  log "binary sha256: $(sha256_short "$BIN_DIR/new-api")"
  local s live=""
  for s in "${SLOTS[@]}"; do
    [[ $(slot_state "$s") == "RUNNING" ]] && live="$s"
  done
  if [[ -n $live ]]; then
    log "live slot: $live -> frontend ${WEB_LINK} -> $(readlink -f "$WEB_LINK" 2>/dev/null || echo '<embedded in binary>')"
    log "  config says: $(slot_static_dir "$live" || echo '<embedded in binary>')"
    log "  serving version: $(served_version "$live")"
  fi
  # `ls -1` rather than `find -printf`, which GNU-only: --status is the first
  # command an operator reaches for when something is wrong, so it must not go
  # blank on the machine they are debugging from.
  if [[ -d $WEB_ROOT ]]; then
    log "frontend bundles: $(ls -1 "$WEB_ROOT" 2>/dev/null | grep -v '^\.staging\.' | tr '\n' ' ')"
  fi
  if [[ -f $STATE_FILE ]]; then
    log "last deploy backup: $(cat "$STATE_FILE")"
  fi
  if [[ -f "$DATA_DIR/loadbalancer.yaml" ]]; then
    log "loadbalancer.yaml modified: $(file_mtime "$DATA_DIR/loadbalancer.yaml")"
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  need_root
  guard_conf_dir

  case "$MODE" in
    status)   show_status; return 0 ;;
    rollback) do_rollback; return 0 ;;
  esac

  need_cmd curl
  need_cmd supervisorctl
  need_cmd flock

  # Before the lock, the backup, or the first byte written: ask the uploaded
  # artifact what it is. Nothing on the host has been touched at this point, so a
  # wrong or unstamped binary costs nothing.
  verify_binary_version "$NEW_BINARY" "$EXPECTED_VERSION"

  mkdir -p "$BIN_DIR" "$BACKUP_DIR" "$LOG_DIR" "$WEB_ROOT" "$DATA_DIR"

  # Ahead of the backup, because a deployment root that has never been used
  # needs its data link before anything tries to read a policy file through it.
  ensure_data_link

  # One deploy at a time: two overlapping runs would race over which slot is
  # live and could stop the wrong process.
  exec 9>"$LOCK_FILE"
  flock -n 9 || die "another deploy is already running ($LOCK_FILE)"

  # Under the lock, so two deploys cannot race each other into registering the
  # same program, and before the backup, so a backup that fails leaves the guard
  # in place rather than the other way round.
  install_slot_guard

  do_backup

  # Install the new binary before touching any process. A running process keeps
  # its open inode, so replacing the file does not disturb the serving slot.
  local staged="$BIN_DIR/new-api.new"
  install -m 0755 "$NEW_BINARY" "$staged"
  mv -f "$staged" "$BIN_DIR/new-api"
  log "installed binary ($(file_size "$BIN_DIR/new-api") bytes, sha256 $(sha256_short "$BIN_DIR/new-api"))"

  # Resolve which slot is live before staging anything: staging must not touch
  # a directory the serving process is reading from.
  legacy_running || ACTIVE_SLOT="$(running_slot || true)"

  # Stage the frontend before anything is bound to it. A bundle that fails
  # validation aborts here, while the old slot is still serving untouched.
  if [[ -n $NEW_WEB ]]; then
    stage_web_bundle "$NEW_WEB"
    NEW_WEB_DIR="$STAGED_WEB_DIR"
  else
    log "no --web bundle given; this deploy serves the frontend embedded in the binary"
  fi

  if legacy_running; then
    if (( BOOTSTRAP == 0 )); then
      die "legacy program '$LEGACY_PROGRAM' is still serving and predates SO_REUSEPORT support.
       One restart with a brief connection gap is unavoidable for this first handover.
       Re-run with --bootstrap when you are ready to accept that."
    fi

    # Bootstrap. The incumbent cannot share the port, so exactly one short
    # window has nothing listening. Stop it before the slots are created:
    # apihub-blue is autostart, and starting it while the legacy process still
    # owns the port would crash-loop it on EADDRINUSE.
    log "BOOTSTRAP: stopping $LEGACY_PROGRAM (expect a short connection gap)"
    sup stop "$LEGACY_PROGRAM"

    source_environment
    write_slot_conf apihub-blue 1 "$NEW_WEB_DIR"
    write_slot_conf apihub-green 0 "$NEW_WEB_DIR"
    # Fatal here in a way it is not elsewhere: the legacy program is already
    # stopped, so a failed reload means nothing is serving and the operator has
    # to read this message to find out.
    apply_supervisor_conf || die "supervisord did not accept the new slot configs; nothing is serving"

    local pid
    pid="$(slot_pid apihub-blue)"
    [[ -n $pid ]] || die "apihub-blue did not start; check $LOG_DIR/apihub-blue.err.log"
    if wait_ready apihub-blue "$pid"; then
      check_reuseport_logged apihub-blue || true
      verify_frontend_served apihub-blue "$NEW_WEB_DIR" || warn "apihub-blue is up but the frontend fingerprint did not match; check APIHUB_STATIC_DIR"
      publish_web_link "$NEW_WEB_DIR" 2>/dev/null || true
      # Same bookkeeping the handover records, so a backup is either complete or
      # absent -- never present-but-missing-fields, which is what made a
      # --rollback after a bootstrap deterministically stop both slots and then
      # refuse to start either.
      printf '%s' "apihub-blue" >"$BACKUP_PATH/deploy-state.slot"
      printf '%s' "$EXPECTED_VERSION" >"$BACKUP_PATH/deploy-state.version"
      printf '%s' "$BACKUP_PATH" >"$STATE_FILE"
      log "bootstrap complete; apihub-blue is live on version $EXPECTED_VERSION"
      log "subsequent deploys are zero-downtime"
      return 0
    fi
    sup stop apihub-blue || true
    die "bootstrap failed; nothing is serving. Restore manually from $BACKUP_PATH"
  fi

  ACTIVE_SLOT="$(running_slot)" || die "neither slot is RUNNING and no legacy program is serving.
       Traffic is already down; start one manually and inspect $BACKUP_PATH"
  NEW_SLOT="$(idle_slot "$ACTIVE_SLOT")"

  local active_pid
  active_pid="$(slot_pid "$ACTIVE_SLOT")"
  log "active slot: $ACTIVE_SLOT (pid $active_pid) -> handing over to $NEW_SLOT"
  check_reuseport_logged "$ACTIVE_SLOT" \
    || warn "$ACTIVE_SLOT may not support reuseport; a first --bootstrap deploy was probably skipped"

  # Only the idle slot's config is touched, so `update` cannot restart the
  # process that is currently serving traffic. The incoming slot is bound to
  # this release's own frontend directory.
  source_environment
  write_slot_conf "$NEW_SLOT" 0 "$NEW_WEB_DIR"
  # Fatal before the handover: $ACTIVE_SLOT is still serving, and starting the
  # incoming slot against a config supervisord never loaded is how a deploy
  # reports success while serving the previous release.
  apply_supervisor_conf || die "supervisord did not accept the $NEW_SLOT config; $ACTIVE_SLOT is untouched and still serving"

  sup start "$NEW_SLOT"
  local new_pid
  new_pid="$(slot_pid "$NEW_SLOT")"
  [[ -n $new_pid ]] || { sup stop "$NEW_SLOT" || true; die "$NEW_SLOT failed to start"; }

  if ! wait_ready "$NEW_SLOT" "$new_pid"; then
    warn "$NEW_SLOT failed readiness; aborting the handover and leaving $ACTIVE_SLOT serving"
    sup stop "$NEW_SLOT" || true
    printf '%s' "$BACKUP_PATH" >"$STATE_FILE"
    die "deploy aborted; $ACTIVE_SLOT is still serving"
  fi
  check_reuseport_logged "$NEW_SLOT" || warn "continuing, but verify $NEW_SLOT really shares the port"

  # Record the outgoing slot before touching it, so --rollback knows where to
  # return to.
  printf '%s' "$ACTIVE_SLOT" >"$BACKUP_PATH/deploy-state.slot"
  printf '%s' "$EXPECTED_VERSION" >"$BACKUP_PATH/deploy-state.version"

  # The new slot is in the group and answering. Drain the old one: SIGTERM
  # triggers the application's own graceful shutdown, which waits for in-flight
  # requests, including long-lived SSE streams, up to SHUTDOWN_TIMEOUT_SECONDS.
  local drain_start; drain_start=$(date +%s)
  log "retiring $ACTIVE_SLOT (pid $active_pid); draining in-flight requests"
  sup stop "$ACTIVE_SLOT" || warn "supervisorctl stop $ACTIVE_SLOT returned non-zero"
  log "$ACTIVE_SLOT drained after $(( $(date +%s) - drain_start ))s"

  # The retired slot is stopped, so its config can now be brought in line with
  # the new version for the next deploy. It gets the same frontend directory:
  # whichever slot serves the next handover must be internally consistent, and
  # the live slot keeps serving the one it started with until it is drained.
  write_slot_conf "$ACTIVE_SLOT" 0 "$NEW_WEB_DIR"
  # A warning, not a failure: $NEW_SLOT is already serving, the retired slot is
  # stopped either way, and its stale config is rewritten again on the next
  # deploy -- where a failed reload IS fatal, before anything has been retired.
  apply_supervisor_conf || warn "the retired $ACTIVE_SLOT config was not reloaded; the next deploy will rewrite it"

  if ! wait_ready "$NEW_SLOT" "$(slot_pid "$NEW_SLOT")"; then
    die "post-handover health check failed on $NEW_SLOT. Previous backup: $BACKUP_PATH"
  fi

  # The old slot is gone, so the port now has a single owner and the answer to
  # "which frontend is this?" is unambiguous. This is the last gate before the
  # deploy is declared good.
  verify_frontend_served "$NEW_SLOT" "$NEW_WEB_DIR" || recover_from_frontend_miss

  publish_web_link "$NEW_WEB_DIR" 2>/dev/null || true
  printf '%s' "$BACKUP_PATH" >"$STATE_FILE"

  # Only now that the release is known good do we discard bundles that are
  # neither live nor reachable by --rollback.
  prune_web_dirs "$NEW_WEB_DIR" "$(cat "$BACKUP_PATH/deploy-state.webdir" 2>/dev/null || true)"

  # Same reasoning for backups/. $STATE_FILE was just rewritten with $BACKUP_PATH,
  # so reading it back yields exactly what --rollback will consume, and that one
  # directory is exempt however many deploys have gone by since.
  prune_backup_dirs "$(cat "$STATE_FILE" 2>/dev/null || true)"

  log "handover complete; $NEW_SLOT serving version $EXPECTED_VERSION (frontend: ${NEW_WEB_DIR:-<embedded in binary>})"
  sup status "${SLOTS[@]}" 2>/dev/null || true
}


# Sourcing with DEPLOY_SH_LIB=1 exposes the helpers above for testing without running
# a deploy. The alternative -- wrapping main in an if -- would put a branch on the
# production path that no real deploy ever exercises.
if [[ ${DEPLOY_SH_LIB:-0} == 1 ]]; then
  return 0 2>/dev/null || exit 0
fi

main "$@"
