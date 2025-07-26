#!/bin/bash

set -e

if ! command -v hyperfine &> /dev/null; then
    echo "Error: hyperfine is not installed."
    exit 1
fi

INPUT_FILE="input.txt"
if [ ! -f "$INPUT_FILE" ]; then
    echo "Input file not found. Downloading Moby Dick from the Gutenberg Project"
    wget https://www.gutenberg.org/files/2701/2701-0.txt -O input.txt
fi

cargo build --release

# Equivalent word count script in Node
cat > word_count.js <<'EOF'
#!/usr/bin/env node

const fs = require('fs');
const readline = require('readline');

function createReader(inputStream) {
  return readline.createInterface({
    input: inputStream,
    terminal: false,
  });
}

function usage() {
  console.error('Usage: node word_count.js [-f filename]');
  process.exit(1);
}

// Parse args
let inputStream = process.stdin;
const args = process.argv.slice(2);

if (args.length > 0) {
  if (args.length !== 2 || args[0] !== '-f') {
    usage();
  }
  const filename = args[1];
  if (!fs.existsSync(filename)) {
    console.error(`File not found: ${filename}`);
    process.exit(1);
  }
  inputStream = fs.createReadStream(filename);
}

const wordCounts = new Map();
const rl = createReader(inputStream);

rl.on('line', (line) => {
  const lower = line.toLowerCase();
  const cleaned = lower.replace(/[^a-z0-9 ]+/g, ' ');
  const words = cleaned.trim().split(/\s+/);

  for (const word of words) {
    if (word.length === 0) continue;
    wordCounts.set(word, (wordCounts.get(word) || 0) + 1);
  }
});

rl.on('close', () => {
  for (const [word, count] of wordCounts) {
    console.log(`${word}: ${count}`);
  }
});
EOF

echo ""
echo "NodeJS version: $(node -v)"
echo "Pulsar version: $(./target/release/pulsar --version)-$(git rev-parse --short HEAD)"
echo "CPU: $(lscpu | grep 'Model name' | sed 's/Model name:\s*//') $(lscpu | grep 'CPU(s):' | head -1 | sed 's/CPU(s):\s*//')"
echo ""

# https://github.com/sharkdp/hyperfine
hyperfine \
    --warmup 3 \
    --runs 10 \
    --export-json benchmark_results.json \
    --export-markdown benchmark_results.md \
    --command-name 'baseline-node-20k-lines' 'node word_count.js -f input.txt' \
    --command-name 'pulsar-20k-lines' './target/release/pulsar -f input.txt > /dev/null' \
    --command-name 'pulsar-20k-lines-sort' './target/release/pulsar -f input.txt --sort > /dev/null' \

# https://github.com/andrewrk/poop
if command -v poop &> /dev/null; then
    poop 'node word_count.js -f input.txt' \
         './target/release/pulsar -f input.txt'
else
    echo "poop is not installed, skipping..."
fi

rm -f word_count.js