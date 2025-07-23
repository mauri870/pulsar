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

cat default_script.js > sort_benchmark.js
cat >> sort_benchmark.js << 'EOF'
const sort = (results) => 
    results.sort((a, b) => a[0].localeCompare(b[0]));
EOF

hyperfine \
  --warmup 3 \
  --runs 10 \
  --export-json benchmark_results.json \
  --export-markdown benchmark_results.md \
  './target/release/pulsar -f input.txt > /dev/null' \
  './target/release/pulsar -f input.txt -s sort_benchmark.js > /dev/null' \
  --command-name 'no-sort,with-sort'

rm -f sort_benchmark.js
