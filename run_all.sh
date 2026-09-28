#!/bin/bash
# Launch helper for the M2 demo. Every process gets its own Terminal window
# so Priya can watch each console, same as running the uv commands by hand.
#
#   ./run_all.sh local              everything on this Mac, in rubric order
#   ./run_all.sh gfd                client laptop: GFD only
#   ./run_all.sh clients            client laptop: C1 C2 C3
#   ./run_all.sh replica 2          replica laptop: LFD2 then S2
#   ./run_all.sh lfd 2              just LFD2
#   ./run_all.sh server 2           just S2
#   ./run_all.sh kill server 1      crash S1 (same effect as Ctrl-C in its window)
#   ./run_all.sh kill all           stop every process this script can find
#
# Add --headless to write each process's output to logs/<name>.log instead of
# opening windows. Handy for rehearsing without ten windows on screen.
#
# Addresses come from .env (see .env.example). Heartbeat rate and client
# interval can be overridden: HEARTBEAT_FREQ=2 INTERVAL=0.5 ./run_all.sh local

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HEARTBEAT_FREQ="${HEARTBEAT_FREQ:-1}"
TIMEOUT="${TIMEOUT:-2}"
INTERVAL="${INTERVAL:-1}"
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

usage() { sed -n '2,20p' "$0"; exit 1; }

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

start_gfd()    { launch "GFD"   "uv run gfd --heartbeat_freq $HEARTBEAT_FREQ --timeout $TIMEOUT"; }
start_lfd()    { launch "LFD$1" "uv run lfd --id LFD$1 --heartbeat_freq $HEARTBEAT_FREQ --timeout $TIMEOUT"; }
start_server() { launch "S$1"   "uv run server --id S$1"; }
start_client() { launch "C$1"   "uv run client --id C$1 --interval $INTERVAL"; }

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
        check_ports_free GFD LFD1 LFD2 LFD3 S1 S2 S3
        start_gfd;               sleep 1
        for n in 1 2 3; do start_lfd "$n"; done;    sleep 1
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
            *)      usage ;;
        esac
        ;;
    *) usage ;;
esac
