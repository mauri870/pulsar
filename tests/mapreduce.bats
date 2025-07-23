setup() {
    BIN="./target/debug/mapreduce"
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
