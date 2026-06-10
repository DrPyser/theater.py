setup() {
  bats_require_minimum_version 1.5.0
  PYTHONUNBUFFERED=1 python3 -m theatre.unix_echo >&2 3>&- &
  SERVER_PID=$!
  for _ in {1..20}; do
    if [ -S /tmp/theatre-echo.sock ]; then
      break
    fi
    sleep 0.25
  done
  if [ ! -S /tmp/theatre-echo.sock ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    fail "server failed to start"
  fi
}

teardown() {
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  rm -f /tmp/theatre-echo.sock
}

@test "server creates socket" {
  [ -S /tmp/theatre-echo.sock ]
}

@test "echo single message" {
  result=$(echo -n "hello" | socat - UNIX-CONNECT:/tmp/theatre-echo.sock)
  [ "$result" = "hello" ]
}

@test "echo multiple messages sequentially" {
  run bash -c 'echo -n "one" | socat -t 1 - UNIX-CONNECT:/tmp/theatre-echo.sock' 3>&-
  [ "$output" = "one" ]
  run bash -c 'echo -n "two" | socat -t 1 - UNIX-CONNECT:/tmp/theatre-echo.sock' 3>&-
  [ "$output" = "two" ]
  run bash -c 'echo -n "three" | socat - UNIX-CONNECT:/tmp/theatre-echo.sock' 3>&-
  [ "$output" = "three" ]
}

@test "echo multiple concurrent clients" {
  [ -S /tmp/theatre-echo.sock ] || fail "socket disappeared"
  out1=$(mktemp -p $BATS_TEST_TMPDIR)
  out2=$(mktemp -p $BATS_TEST_TMPDIR)

  echo -n "alpha" | timeout 2 socat -t 1 - UNIX-CONNECT:/tmp/theatre-echo.sock > "$out1" &
  pid1=$!
  echo -n "beta" | timeout 2 socat -t 1 - UNIX-CONNECT:/tmp/theatre-echo.sock > "$out2" &
  pid2=$!

  wait "$pid1" "$pid2"

  output_a=$(cat "$out1")
  output_b=$(cat "$out2")

  [ "$output_a" = "alpha" ]
  [ "$output_b" = "beta" ]
}

@test "echo large message" {
  result=$(head -c 65536 /dev/zero | socat - UNIX-CONNECT:/tmp/theatre-echo.sock | wc -c)
  [ "$result" -eq 65536 ]
}

@test "server handles binary data" {
  result=$(printf '\x00\x01\x02\xff\xfe\xfd' | socat - UNIX-CONNECT:/tmp/theatre-echo.sock | od -An -tx1 | tr -d '[:space:]')
  [ "$result" = "000102fffefd" ]
}
