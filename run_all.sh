#!/bin/bash
# Launch helper for the M2 and M3 demos. Every process gets its own Terminal
# window so Priya can watch each console, same as running the uv commands by
# hand. The replication mode (active for M2, passive for M3) comes from
# REPLICATION_MODE in .env; flipping that single value is enough to switch the
# demo this script runs.
#
#   ./run_all.sh local              everything on this Mac, in rubric order
#   ./run_all.sh local m3           same, but force passive mode (M3) for this run
#   ./run_all.sh local m2           same, but force active mode (M2) for this run
#   ./run_all.sh gfd                client laptop: GFD only
#   ./run_all.sh clients            client laptop: C1 C2 C3
#   ./run_all.sh replica 2          replica laptop: LFD2 then S2
#   ./run_all.sh lfd 2              just LFD2
#   ./run_all.sh server 2           just S2
#   ./run_all.sh kill server 1      crash S1 (same effect as Ctrl-C in its window)
#   ./run_all.sh kill server 2      M3 first fault: kill backup S2
#   ./run_all.sh kill server 3      M3 second fault: kill backup S3 after S2
#   ./run_all.sh kill lfd 1         crash LFD1; also works for client 1 and gfd
#   ./run_all.sh kill all           stop every process this script can find
#
# Add --headless to write each process's output to logs/<name>.log instead of
# opening windows. Handy for rehearsing without ten windows on screen.
#
# Addresses, replication mode, primary, and checkpoint frequency come from
# .env (see .env.example). Shell overrides win over .env:
#   HEARTBEAT_FREQ=2 INTERVAL=0.5 ./run_all.sh local
#   CHECKPOINT_FREQ=2 ./run_all.sh local m3
#   REPLICATION_MODE=passive ./run_all.sh local

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HEARTBEAT_FREQ="${HEARTBEAT_FREQ:-1}"
TIMEOUT="${TIMEOUT:-2}"
INTERVAL="${INTERVAL:-1}"
# Project guide: checkpoint_freq is a CLI parameter, never hardcoded in the
# server. Default 1 Hz (one checkpoint per second); override via the
# environment or .env. REPLICATION_MODE / PRIMARY_ID stay optional here so
# config.py can still take them from .env when this script does not export them.
CHECKPOINT_FREQ="${CHECKPOINT_FREQ:-1}"
HEADLESS=0
LOGDIR="$ROOT/logs"

# Port a process listens on. Same lookup order as config.py: an exported
# shell variable wins, then .env, then the built-in default.
port_of() {
    local name="$1" var="${1}_PORT" value default
    case "$name" in
        GFD) default=9001 ;;
        S1) default=8080 ;; S2) default=8082 ;; S3) default=8083 ;;
        LFD1) default=8081 ;; LFD2) default=8084 ;; LFD3) default=8085 ;;
    esac
    value="${!var:-}"
    if [ -z "$value" ]; then
        value=$(grep -E "^${var}=" "$ROOT/.env" 2>/dev/null | tail -1 | cut -d= -f2 | tr -d '"' | tr -d "'")
    fi
    echo "${value:-$default}"
}

usage() { sed -n '2,29p' "$0"; exit 1; }

# Prepend a child-process env prefix so the terminal that runs `uv run ...`
# inherits our shell overrides even when launched through osascript. config.py
# calls load_dotenv() without override, so a non-empty value here beats .env.
env_prefix() {
    local out=""
    [ -n "${REPLICATION_MODE:-}" ] && out="REPLICATION_MODE=$REPLICATION_MODE "
    [ -n "${CHECKPOINT_FREQ:-}" ]  && out="${out}CHECKPOINT_FREQ=$CHECKPOINT_FREQ "
    [ -n "${PRIMARY_ID:-}" ]       && out="${out}PRIMARY_ID=$PRIMARY_ID "
    printf "%s" "$out"
}

# Open one Terminal window running `cmd`, titled `name`. In headless mode
# run it in the background and log to logs/name.log instead.
launch() {
    local name="$1" cmd="$2"
    if [ "$HEADLESS" = 1 ]; then
        mkdir -p "$LOGDIR"
        (cd "$ROOT" && eval "$cmd") > "$LOGDIR/$name.log" 2>&1 &
        echo "started $name (pid $!) -> logs/$name.log"
    else
        # If Terminal is still relaunching after a `kill all`, the first attempt
        # can fail with "Connection is invalid"; wait a moment and try once more.
        local attempt
        for attempt in 1 2; do
            if osascript > /dev/null 2>&1 <<EOF
tell application "Terminal"
    activate
    -- The printf sets the window title to the process name before the command runs.
    set t to do script "cd '$ROOT' && printf '\\\\033]0;$name\\\\007' && $cmd"
    -- Hide the other title parts Terminal would add (working directory, shell, size).
    set custom title of t to "$name"
    set title displays custom title of t to true
    set title displays device name of t to false
    set title displays file name of t to false
    set title displays shell path of t to false
    set title displays window size of t to false
end tell
EOF
            then
                echo "opened window: $name"
                return
            fi
            sleep 1
        done
        echo "could not open a Terminal window for $name"
    fi
}

# Visible on the process command line so Priya can see the configurable
# checkpoint_freq (and, in M3, the primary id) without opening the source.
server_flags() {
    local flags="--checkpoint-freq $CHECKPOINT_FREQ"
    [ -n "${REPLICATION_MODE:-}" ] && flags="--replication-mode $REPLICATION_MODE $flags"
    [ -n "${PRIMARY_ID:-}" ] && flags="$flags --primary $PRIMARY_ID"
    printf "%s" "$flags"
}

client_flags() {
    local flags="--interval $INTERVAL"
    [ -n "${REPLICATION_MODE:-}" ] && flags="--replication-mode $REPLICATION_MODE $flags"
    printf "%s" "$flags"
}

start_gfd()    { launch "GFD"   "$(env_prefix)uv run gfd --heartbeat_freq $HEARTBEAT_FREQ --timeout $TIMEOUT"; }
start_lfd()    { launch "LFD$1" "$(env_prefix)uv run lfd --id LFD$1 --heartbeat_freq $HEARTBEAT_FREQ --timeout $TIMEOUT"; }
start_server() { launch "S$1"   "$(env_prefix)uv run server --id S$1 $(server_flags)"; }
start_client() { launch "C$1"   "$(env_prefix)uv run client --id C$1 $(client_flags)"; }

# Crash one of our processes by finding what is listening on its port.
# Only kills something launched from this repo's .venv; anything else on the
# port (OrbStack likes 8080, for example) is left alone and reported.
kill_port() {
    local name="$1" port="$2" pids pid cmd
    pids=$(lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null)
    if [ -z "$pids" ]; then
        echo "$name: nothing listening on port $port"
        return
    fi
    for pid in $pids; do
        cmd=$(ps -o command= -p "$pid" 2>/dev/null)
        case "$cmd" in
            *"$ROOT/.venv/bin/"*) ;;
            *) echo "$name: port $port is held by something that isn't ours, leaving it alone:"
               echo "    pid $pid: $cmd"
               continue ;;
        esac
        kill -INT "$pid" 2>/dev/null
        sleep 0.5
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
        echo "killed $name (pid $pid, port $port)"
    done
}

# Refuse to launch on top of leftovers from an earlier run.
check_ports_free() {
    local busy=0 name port
    for name in "$@"; do
        port=$(port_of "$name")
        if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            echo "port $port ($name) is already in use:"
            lsof -iTCP:"$port" -sTCP:LISTEN | tail -n +2
            busy=1
        fi
    done
    if [ "$busy" = 1 ]; then
        echo "run './run_all.sh kill all' first."
        exit 1
    fi
}

kill_all() {
    kill_port GFD "$(port_of GFD)"
    for n in 1 2 3; do
        kill_port "S$n" "$(port_of S$n)"
        kill_port "LFD$n" "$(port_of LFD$n)"
    done
    # Clients do not listen, so match their command line.
    pkill -f '\.venv/bin/client --id' 2>/dev/null && echo "killed clients"
    true
}

# Parse args. --headless can appear anywhere.
ARGS=()
for a in "$@"; do
    case "$a" in
        --headless) HEADLESS=1 ;;
        *) ARGS+=("$a") ;;
    esac
done
set -- "${ARGS[@]+"${ARGS[@]}"}"
[ $# -ge 1 ] || usage

if [ ! -f "$ROOT/.env" ] && [ "$1" != "kill" ]; then
    echo "no .env found; using config.py defaults (everything on 127.0.0.1)."
    echo "for a multi-laptop run: cp .env.example .env and fill in real IPs."
fi

case "$1" in
    local)
        # Optional second arg forces the replication mode for this run only.
        case "${2:-}" in
            m2|active)  export REPLICATION_MODE=active ;;
            m3|passive)
                export REPLICATION_MODE=passive
                # Guide: S1 is primary because it is the first replica to show up.
                export PRIMARY_ID="${PRIMARY_ID:-S1}"
                ;;
            "")         ;;  # keep whatever REPLICATION_MODE / .env says
            *) echo "unknown local mode: $2 (want m2|m3|active|passive)"; exit 1 ;;
        esac
        check_ports_free GFD LFD1 LFD2 LFD3 S1 S2 S3
        start_gfd;               sleep 1
        for n in 1 2 3; do start_lfd "$n"; done;    sleep 1
        # Start S1 first so, in passive mode, the primary can open its outbound
        # checkpoint channel to S2 and S3 as soon as their listen sockets exist.
        for n in 1 2 3; do start_server "$n"; sleep 1; done
        sleep 1
        for n in 1 2 3; do start_client "$n"; sleep 1; done
        ;;
    gfd)      check_ports_free GFD; start_gfd ;;
    clients)  for n in 1 2 3; do start_client "$n"; sleep 1; done ;;
    replica)  [ $# -eq 2 ] || usage; check_ports_free "LFD$2" "S$2"; start_lfd "$2"; sleep 1; start_server "$2" ;;
    lfd)      [ $# -eq 2 ] || usage; check_ports_free "LFD$2"; start_lfd "$2" ;;
    server)   [ $# -eq 2 ] || usage; check_ports_free "S$2"; start_server "$2" ;;
    client)   [ $# -eq 2 ] || usage; start_client "$2" ;;
    kill)
        case "${2:-}" in
            all)    kill_all ;;
            server) [ $# -eq 3 ] || usage; kill_port "S$3" "$(port_of S$3)" ;;
            lfd)    [ $# -eq 3 ] || usage; kill_port "LFD$3" "$(port_of LFD$3)" ;;
            gfd)    kill_port GFD "$(port_of GFD)" ;;
            client) [ $# -eq 3 ] || usage
                    # Clients don't listen on a port, so match the command line instead.
                    pattern="$ROOT/.venv/bin/client --id C$3( |\$)"
                    if pkill -INT -f "$pattern"; then
                        sleep 0.5
                        pkill -9 -f "$pattern" 2>/dev/null
                        echo "killed C$3"
                    else
                        echo "C$3: not running"
                    fi ;;
            *)      usage ;;
        esac
        ;;
    *) usage ;;
esac
