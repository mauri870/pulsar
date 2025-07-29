BIN="./target/release/pulsar"

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
async function map(line) {
  return [[line, parseInt(line) * 2]];
}

async function reduce(key, values) {
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

@test "script with async functions" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  cat > "$SCRIPTFILE" << 'EOF'
async function map(line) {
  return [[line, parseInt(line) * 2]];
}

async function reduce(key, values) {
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

@test "custom script with const arrow functions" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  # custom script that doubles the value of each line
  cat > "$SCRIPTFILE" << 'EOF'
const map = async (line) => [[line, parseInt(line) * 2]];
var reduce = async (key, values) => values[0];
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

@test "sorting" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  cat > "$SCRIPTFILE" << 'EOF'
const map = async (line) => [[line, 0]];
const reduce = async (key, values) => values[0];
const sort = async results => results.sort((a, b) => b[0].localeCompare(a[0]))
EOF

  "$BIN" -f "$TESTFILE" -s "$SCRIPTFILE" --sort > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  [[ $(echo "$output" | wc -l) -eq 4 ]]
  [[ "$output" == "3: 0
2: 0
1: 0
0: 0" ]]

  rm -rf "$TMPDIR"
}

@test "map reduce returning objects" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  cat > "$SCRIPTFILE" << 'EOF'
const map = async (line) => {
  const num = parseInt(line);
  const category = num % 2 === 0 ? "even" : "odd";
  return [[category, { line }]];
};

const reduce = async (key, values) => {
  const sorted = values
    .map(v => parseInt(v.line))
    .sort((a, b) => a - b);
  return { reduction: sorted.join(",") };
};
EOF

  # Test with custom script file
  "$BIN" -f "$TESTFILE" -s "$SCRIPTFILE" --output=json > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  [[ $(echo "$output" | wc -l) -eq 2 ]]
  [[ "$output" =~ '{"even":{"reduction":"0,2"}}' ]]
  [[ "$output" =~ '{"odd":{"reduction":"1,3"}}' ]]

  rm -rf "$TMPDIR"
}

@test "map reduce returning strings" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  # custom script that returns string values instead of numbers
  cat > "$SCRIPTFILE" << 'EOF'
const map = async (line) => {
  const num = parseInt(line);
  const category = num % 2 === 0 ? "even" : "odd";
  return [[category, line]];
};

const reduce = async (key, values) => {
  return values.sort((a, b) => parseInt(a) - parseInt(b)).join(",");
};

const sort = async (results) => results.sort((a, b) => a[0].localeCompare(b[0]));
EOF

  # Test with custom script file
  "$BIN" -f "$TESTFILE" -s "$SCRIPTFILE" --sort > "$OUTFILE"

  run cat "$OUTFILE"
  [ "$status" -eq 0 ]

  [[ $(echo "$output" | wc -l) -eq 2 ]]
  [[ "$output" =~ "even: 0,2" ]]
  [[ "$output" =~ "odd: 1,3" ]]

  rm -rf "$TMPDIR"
}

@test "errors out on js syntax error" {
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
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Error loading JS code: Error: unexpected token in expression: 'var'" ]]
  rm -rf "$TMPDIR"
}

@test "runs test script" {
  TMPDIR=$(mktemp -d)
  TESTFILE="$TMPDIR/test.txt"
  SCRIPTFILE="$TMPDIR/script.js"
  OUTFILE="$TMPDIR/out.txt"

  echo -e "0\n1\n2\n3" > "$TESTFILE"

  cat > "$SCRIPTFILE" << 'EOF'
const map = async line => line.split(' ').map(word => [word, 1]);

const reduce = async (key, values) => values.length;

var test = async () => {
  const input = "The quick brown fox jumps over the lazy dog";
  const result = await map(input);
  const expectedWords = [
    "the", "quick", "brown", "fox",
    "jumps", "over", "the", "lazy", "dog"
  ];
  if (result.length !== expectedWords.length) {
    throw new Error("incorrect number of words, wanted " + expectedWords.length + ", got " + result.length);
  }
};
EOF
  run "$BIN" -f "$TESTFILE" -s "$SCRIPTFILE" --test
  [ "$status" -eq 0 ]
  [[ "$output" =~ "OK" ]]

  cat > "$SCRIPTFILE" << 'EOF'
const map = async line => line.split(' ').map(word => [word, 1]);

const reduce = async (key, values) => values.length;

var test = async () => {
  const input = "The quick brown fox jumps over the lazy dog";
  const result = await map(input);
  if (result.length !== -1) { // Just so it fails
    throw new Error("this is a test failure");
  }
};
EOF
  run "$BIN" -f "$TESTFILE" -s "$SCRIPTFILE" --test
  [ "$status" -eq 1 ]
  [[ "$output" =~ "this is a test failure" ]]
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

  [[ "$output" =~ '{"hello":2}' ]]
  [[ "$output" =~ '{"world":1}' ]]

  rm -rf "$TMPDIR"
}
