#!/usr/bin/env bash
set -u

TPCH_HOME="${TPCH_HOME:-/tmp/altibase-tpch-perf}"
QUERY_DIR="${QUERY_DIR:-$TPCH_HOME/queries}"
RESULT_DIR="${RESULT_DIR:-$TPCH_HOME/results}"
LOG_DIR="${LOG_DIR:-$TPCH_HOME/logs}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"

PRE_DELAY_SECONDS="${PRE_DELAY_SECONDS:-30}"
INTER_CASE_DELAY_SECONDS="${INTER_CASE_DELAY_SECONDS:-30}"
POST_DELAY_SECONDS="${POST_DELAY_SECONDS:-30}"
ENABLE_CLIENT_VMSTAT="${ENABLE_CLIENT_VMSTAT:-true}"

RESULT_CSV="$RESULT_DIR/tpch-results-$RUN_ID.csv"
TIMELINE_CSV="$RESULT_DIR/tpch-timeline-$RUN_ID.csv"
CLIENT_VMSTAT_LOG="$RESULT_DIR/client-vmstat-$RUN_ID.log"
RUN_LOG_DIR="$LOG_DIR/$RUN_ID"

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "Missing required environment variable: $name" >&2
    exit 2
  fi
}

csv_escape() {
  local value="${1:-}"
  value="${value//\"/\"\"}"
  printf '"%s"' "$value"
}

csv_line() {
  local first=true
  for value in "$@"; do
    if [ "$first" = true ]; then
      first=false
    else
      printf ','
    fi
    csv_escape "$value"
  done
  printf '\n'
}

epoch_seconds() {
  date +%s
}

epoch_nanoseconds() {
  date +%s%N
}

wall_time() {
  date '+%Y-%m-%d %H:%M:%S %Z'
}

elapsed_seconds() {
  local start_ns="$1"
  local end_ns="$2"
  awk "BEGIN { printf \"%.6f\", ($end_ns - $start_ns) / 1000000000 }"
}

timeline_event() {
  local event="$1"
  local query="${2:-}"
  local detail="${3:-}"
  csv_line "$event" "$query" "$(epoch_seconds)" "$(wall_time)" "$detail" >> "$TIMELINE_CSV"
}

stop_client_vmstat() {
  if [ -n "${CLIENT_VMSTAT_PID:-}" ] && kill -0 "$CLIENT_VMSTAT_PID" 2>/dev/null; then
    kill "$CLIENT_VMSTAT_PID" 2>/dev/null || true
    wait "$CLIENT_VMSTAT_PID" 2>/dev/null || true
    timeline_event "client_vmstat_stop" "" "$CLIENT_VMSTAT_LOG"
  fi
}

require_env ISQL
require_env DB_HOST
require_env DB_PORT
require_env DB_USER
require_env DB_PASS

if [ ! -x "$ISQL" ]; then
  echo "ISQL is not executable: $ISQL" >&2
  exit 2
fi

mkdir -p "$RESULT_DIR" "$RUN_LOG_DIR"

csv_line "query" "query_file" "start_epoch" "start_time" "end_epoch" "end_time" "elapsed_sec" "exit_code" "stdout_file" "stderr_file" > "$RESULT_CSV"
csv_line "event" "query" "epoch" "time" "detail" > "$TIMELINE_CSV"

timeline_event "run_start" "" "run_id=$RUN_ID"

if [ "$ENABLE_CLIENT_VMSTAT" = "true" ]; then
  vmstat -w -t 1 > "$CLIENT_VMSTAT_LOG" &
  CLIENT_VMSTAT_PID="$!"
  trap stop_client_vmstat EXIT INT TERM
  timeline_event "client_vmstat_start" "" "$CLIENT_VMSTAT_LOG"
fi

timeline_event "pre_delay_start" "" "${PRE_DELAY_SECONDS}s"
sleep "$PRE_DELAY_SECONDS"
timeline_event "pre_delay_end" "" "${PRE_DELAY_SECONDS}s"

exit_status=0
query_count=0

for sql in "$QUERY_DIR"/tpch-q*.sql; do
  if [ ! -f "$sql" ]; then
    continue
  fi

  query_count=$((query_count + 1))
  name="$(basename "$sql" .sql)"
  stdout_file="$RUN_LOG_DIR/${name}.out"
  stderr_file="$RUN_LOG_DIR/${name}.err"

  echo "running $name"
  start_epoch="$(epoch_seconds)"
  start_time="$(wall_time)"
  start_ns="$(epoch_nanoseconds)"
  timeline_event "query_start" "$name" "$sql"

  {
    cat "$sql"
    printf '\nexit;\n'
  } | "$ISQL" -s "$DB_HOST" -u "$DB_USER" -p "$DB_PASS" -port "$DB_PORT" > "$stdout_file" 2> "$stderr_file"
  code="$?"

  end_ns="$(epoch_nanoseconds)"
  end_epoch="$(epoch_seconds)"
  end_time="$(wall_time)"
  elapsed="$(elapsed_seconds "$start_ns" "$end_ns")"
  timeline_event "query_end" "$name" "exit_code=$code elapsed_sec=$elapsed"

  csv_line "$name" "$sql" "$start_epoch" "$start_time" "$end_epoch" "$end_time" "$elapsed" "$code" "$stdout_file" "$stderr_file" >> "$RESULT_CSV"

  if [ "$code" -ne 0 ]; then
    exit_status="$code"
  fi

  next_sql="$(find "$QUERY_DIR" -maxdepth 1 -name 'tpch-q*.sql' | sort | awk -v current="$sql" 'found { print; exit } $0 == current { found=1 }')"
  if [ -n "$next_sql" ]; then
    timeline_event "inter_case_delay_start" "$name" "${INTER_CASE_DELAY_SECONDS}s"
    sleep "$INTER_CASE_DELAY_SECONDS"
    timeline_event "inter_case_delay_end" "$name" "${INTER_CASE_DELAY_SECONDS}s"
  fi
done

timeline_event "post_delay_start" "" "${POST_DELAY_SECONDS}s"
sleep "$POST_DELAY_SECONDS"
timeline_event "post_delay_end" "" "${POST_DELAY_SECONDS}s"

stop_client_vmstat
trap - EXIT INT TERM

timeline_event "run_end" "" "run_id=$RUN_ID query_count=$query_count exit_status=$exit_status"

echo "result_csv=$RESULT_CSV"
echo "timeline_csv=$TIMELINE_CSV"
echo "client_vmstat_log=$CLIENT_VMSTAT_LOG"
echo "log_dir=$RUN_LOG_DIR"

exit "$exit_status"
