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

# Create AWK script for word counting
cat > word_count.awk << 'EOF'
#!/usr/bin/awk -f

# Word count script - reads from stdin line by line
# Converts to lowercase, removes punctuation, splits on whitespace
# and counts occurrences of each word

{
    # Convert line to lowercase
    line = tolower($0)
    
    # Remove punctuation and non-alphanumeric characters (keep only letters, numbers, spaces)
    gsub(/[^a-z0-9 ]/, " ", line)
    
    # Split line into words and count each word
    n = split(line, words, /[ \t]+/)
    for (i = 1; i <= n; i++) {
        word = words[i]
        if (word != "") {  # Skip empty strings
            count[word]++
        }
    }
}

END {
    # Print word counts
    for (word in count) {
        print word ": " count[word]
    }
}
EOF

# https://github.com/sharkdp/hyperfine
hyperfine \
    --warmup 3 \
    --runs 10 \
    --export-json benchmark_results.json \
    --export-markdown benchmark_results.md \
    --command-name 'baseline-awk-20k-lines' 'awk -f word_count.awk input.txt' \
    --command-name 'pulsar-20k-lines' './target/release/pulsar -f input.txt > /dev/null' \
    --command-name 'pulsar-20k-lines-sort' './target/release/pulsar -f input.txt --sort > /dev/null' \

# https://github.com/andrewrk/poop
if command -v poop &> /dev/null; then
    poop 'awk -f word_count.awk input.txt' \
         './target/release/pulsar -f input.txt'
else
    echo "poop is not installed, skipping..."
fi

rm -f sort_benchmark.js word_count.awk