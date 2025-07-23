BIN="./target/release/mapreduce"

setup_file() {
  cargo b --release
}

diag() {
  echo "# DEBUG $@" >&3
}

@test "file input" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  echo "The quick brown fox jumps over the lazy dog" > "$TESTFILE"

  OUTFILE="$TMPDIR/out.txt"

  # Test with file input
  "$BIN" -f "$TESTFILE" > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  # Check that we get word counts for each word
  [[ "$(echo "$output" | wc -l)" -eq 8 ]]
  [[ "$output" =~ "the: 2" ]]
  [[ "$output" =~ "quick: 1" ]]
  [[ "$output" =~ "brown: 1" ]]
  [[ "$output" =~ "fox: 1" ]]
  [[ "$output" =~ "jumps: 1" ]]
  [[ "$output" =~ "over: 1" ]]
  [[ "$output" =~ "lazy: 1" ]]
  [[ "$output" =~ "dog: 1" ]]

  rm -rf "$TMPDIR"
}

@test "stdin input" {
  TMPDIR=$(mktemp -d)
  OUTFILE="$TMPDIR/out.txt"

  # Test with stdin input
  echo "hello world hello" | "$BIN" > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  # Check that we get correct word counts
  [[ "$output" =~ "hello: 2" ]]
  [[ "$output" =~ "world: 1" ]]

  rm -rf "$TMPDIR"
}

@test "custom script file" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  # custom script that doubles the value of each line
  cat > "$SCRIPTFILE" << 'EOF'
function map(line) {
  return [[line, parseInt(line) * 2]];
}

function reduce(key, values) {
  return values[0];
}
EOF

  # Test with custom script file
  "$BIN" -f "$TESTFILE" -s "$SCRIPTFILE" > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  [[ $(echo "$output" | wc -l) -eq 4 ]]
  [[ "$output" =~ "0: 0" ]]
  [[ "$output" =~ "1: 2" ]]
  [[ "$output" =~ "2: 4" ]]
  [[ "$output" =~ "3: 6" ]]

  rm -rf "$TMPDIR"
}

@test "custom script with const and arrow functions" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  # custom script that doubles the value of each line
  cat > "$SCRIPTFILE" << 'EOF'
const map = (line) => [[line, parseInt(line) * 2]];
var reduce = (key, values) => values[0];
EOF

  # Test with custom script file
  "$BIN" -f "$TESTFILE" -s "$SCRIPTFILE" > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  [[ $(echo "$output" | wc -l) -eq 4 ]]
  [[ "$output" =~ "0: 0" ]]
  [[ "$output" =~ "1: 2" ]]
  [[ "$output" =~ "2: 4" ]]
  [[ "$output" =~ "3: 6" ]]

  rm -rf "$TMPDIR"
}

@test "custom script with syntax error" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  cat > "$SCRIPTFILE" << 'EOF'
const map =
var reduce = (key, values) => values[0];
EOF

  # Test with custom script file
  run "$BIN" -f "$TESTFILE" -s "$SCRIPTFILE"
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Failed to evaluate js script: Exception" ]]
  rm -rf "$TMPDIR"
}

@test "output plain" {
  TMPDIR=$(mktemp -d)
  OUTFILE="$TMPDIR/out.txt"

  # Test with stdin input
  echo "hello world hello" | "$BIN" --output=plain > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  # Check that we get correct word counts
  [[ "$output" =~ "hello: 2" ]]
  [[ "$output" =~ "world: 1" ]]

  rm -rf "$TMPDIR"
}

@test "output json" {
  TMPDIR=$(mktemp -d)
  OUTFILE="$TMPDIR/out.txt"

  # Test with stdin input
  echo "hello world hello" | "$BIN" --output=json > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  # Check that we get correct word counts
  [[ "$output" =~ '{"hello": 2}' ]]
  [[ "$output" =~ '{"world": 1}' ]]

  rm -rf "$TMPDIR"
}
