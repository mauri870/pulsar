#!/bin/bash
# Creates the benchmark inputs in bench/data (ignored by git):
#   text.txt      about 98 MB of English text (Moby Dick repeated)
#   distinct.txt  3 million lines that are all different words
#   moby.txt      the single copy of the text, for the latency workload
set -e
cd "$(dirname "$0")"
mkdir -p data

if [ ! -f data/moby.txt ]; then
    if [ -f ../input.txt ]; then
        cp ../input.txt data/moby.txt
    else
        curl -fsSL https://www.gutenberg.org/files/2701/2701-0.txt -o data/moby.txt
    fi
fi

if [ ! -f data/text.txt ]; then
    for _ in $(seq 1 80); do cat data/moby.txt; done > data/text.txt
fi

if [ ! -f data/distinct.txt ]; then
    seq 1 3000000 > data/distinct.txt
fi
