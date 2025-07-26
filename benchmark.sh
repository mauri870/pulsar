#!/bin/bash

set -e

if ! command -v hyperfine &> /dev/null; then
    echo "Error: hyperfine is not installed."
    exit 1
fi

WITH_NODE=0
for arg in "$@"; do
  if [[ "$arg" == "--with-node" ]]; then
    WITH_NODE=1
  fi
done

echo ""
[[ $WITH_NODE -eq 1 ]] && echo "NodeJS version: $(node -v)"
echo "Pulsar version: $(./target/release/pulsar --version)-$(git rev-parse --short HEAD)"
echo "CPU: $(lscpu | grep 'Model name' | sed 's/Model name:\s*//') $(lscpu | grep 'CPU(s):' | head -1 | sed 's/CPU(s):\s*//')"
echo ""

cat <<EOF
Summary

This benchmark performs a simple word count aggregation on a 20,000-line
copy of the Moby Dick by Herman Melville.

Each line is processed by the map function, which introduces an artificial
delay of approximately 0.23 ms per line, to simulate processing.

EOF

[[ $WITH_NODE -eq 1 ]] && cat <<EOF
It compares Pulsar against a NodeJS equivalent implementation. Both
versions are asynchronous but, due to the nature of NodeJS, it runs on a
single thread. Remember, concurrency is not parallelism.

Pulsar, on the other hand, is a highly parallel MapReduce engine and can
leverage multiple threads and multiple execution contexts.

EOF

INPUT_FILE="input.txt"
if [ ! -f "$INPUT_FILE" ]; then
    echo "Input file not found. Downloading Moby Dick from the Gutenberg Project"
    wget https://www.gutenberg.org/files/2701/2701-0.txt -O input.txt
fi

cargo build --release

cat > node-script.js <<'EOF'
const fs = require('fs');
const readline = require('readline');

function createReader(inputStream) {
  return readline.createInterface({
    input: inputStream,
    terminal: false,
  });
}

// Parse args
let inputStream = process.stdin;
const args = process.argv.slice(1);

if (args.length > 0) {
  const filename = args[1];
  if (!fs.existsSync(filename)) {
    console.error(`File not found: ${filename}`);
    process.exit(1);
  }
  inputStream = fs.createReadStream(filename);
}

const wordCounts = new Map();

async function processLine(line) {
  const lower = line.toLowerCase();
  const cleaned = lower.replace(/[^a-z0-9 ]+/g, ' ');
  const words = cleaned.trim().split(/\s+/);

  // Simulate processing delay once per line
  await doWork();

  for (const word of words) {
    if (word.length === 0) continue;
    wordCounts.set(word, (wordCounts.get(word) || 0) + 1);
  }
}

const doWork = async () => {
  const start = performance.now();
  while ((performance.now() - start) < 0.23) {
    await new Promise(resolve => setImmediate(resolve));
  }
}

(async () => {
  const rl = createReader(inputStream);

  for await (const line of rl) {
    await processLine(line);
  }

  for (const [word, count] of wordCounts) {
    console.log(`${word}: ${count}`);
  }
})();
EOF

cat > pulsar-script.js <<'EOF'
const map = async line => {
  // Simulate processing delay once per line
  await doWork();

  return line
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ') // keep only letters and numbers
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map(word => [word, 1]);
};

const doWork = async () => {
  const start = performance.now();
  while ((performance.now() - start) < 0.23) {
    await new Promise(resolve => setImmediate(resolve));
  }
}

const reduce = async (key, values) => values.length; // count occurrences of each word

const sort = async (results) =>
    results.sort((a, b) => a[0].localeCompare(b[0])) // Sort alphabetically
EOF

HYPERFINE_ARGS=(
    --warmup 1
    --runs 5
    --command-name 'pulsar-20k-lines' './target/release/pulsar -f input.txt -s pulsar-script.js'
    --command-name 'pulsar-20k-lines-sort-by-key-asc' './target/release/pulsar -f input.txt -s pulsar-script.js --sort'
)

POOP_ARGS=(
    './target/release/pulsar -f input.txt -s pulsar-script.js'
    './target/release/pulsar -f input.txt -s pulsar-script.js --sort'
)

if [[ $WITH_NODE -eq 1 ]]; then
    HYPERFINE_ARGS+=( --command-name 'baseline-node-20k-lines' 'node node-script.js input.txt' )
    POOP_ARGS+=('node node-script.js input.txt')
fi

# https://github.com/sharkdp/hyperfine
hyperfine "${HYPERFINE_ARGS[@]}"

# https://github.com/andrewrk/poop
if command -v poop &> /dev/null; then
    poop "${POOP_ARGS[@]}"
else
    echo "poop is not installed, skipping..."
fi

rm -f node-script.js pulsar-script.js
