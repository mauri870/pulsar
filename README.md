# pulsar

`pulsar` is a CLI tool that provides a high-performance MapReduce engine for processing large datasets using user-defined JavaScript functions.

Features include parallel processing powered by Tokio, robust JavaScript support via [Amazon AWS's LLRT](https://github.com/awslabs/llrt) engine (based on [QuickJS](https://github.com/DelSkayn/rquickjs)), support for streaming output, NDJSON output, and sorting.

By default, if no JS script is provided, it performs a simple word count. See `default_script.js` for the default behavior and available options.

## Compilation

```bash
cargo build --release
```

The binary will be located at ./target/release/pulsar.

## Usage

```bash
./target/release/pulsar -f input_file -s script_file
```

## Examples

### Word Count (Default)

Counting the words in a text file:

```bash
# download Moby Dick from Gutenberg
$ wget https://www.gutenberg.org/files/2701/2701-0.txt -O input.txt
$ wc -l input.txt
21940

$ time cat input.txt | ./target/release/pulsar
...
bluish: 2
pedlar: 1
magazine: 2
reckless: 5

real	0m0.282s
user	0m2.020s
sys	  0m0.270s
```

You could provide a script to ignore stop words and sort the results:

```bash
cat > script.js << 'EOF'
const STOP_WORDS = new Set([
  "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
  "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
  "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"
]);

const map = async line => line
  .toLowerCase()
  .replace(/[^\p{L}\p{N}]+/gu, ' ')
  .trim()
  .split(/\s+/)
  .filter(word => word && !STOP_WORDS.has(word))
  .map(word => [word, 1]);

const reduce = async (key, values) => values.length;

const sort = async results =>
  results.sort((a, b) => a[0].localeCompare(b[0])); // Sort alphabetically
EOF

./target/release/pulsar -f input.txt -s script.js
```

### Log Analysis

Summarize web server logs to count logs per status codes:

```bash
# generate logs
docker run --rm mingrammer/flog -n 1000 > /tmp/access.log

cat > script.js << 'EOF'
const map = async line => {
  // Parse Apache/Nginx log line example:
  // 127.0.0.1 - - [01/Jan/2023:00:00:01 +0000] "GET /path HTTP/1.1" 200 1234
  // Extract the HTTP status code (e.g. 200)
  const match = line.match(/"\w+ \S+ \S+" (\d{3}) \d+/);
  if (match?.[1]) {
    const status = match[1];
    return [[status, 1]];
  }
  return [];
};

const reduce = async (key, values) =>
  values.reduce((sum, count) => sum + count, 0);
EOF

./target/release/pulsar -f /tmp/access.log -s script.js
```

You could build on this to aggregate local vs internet IPs, then print the results in json:

```bash
cat > script.js << 'EOF'
const isLocal = ip => {
  const [a, b] = ip.split('.').map(Number);
  return a === 10 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || a === 127;
};

const map = async line =>
  [...line.matchAll(/\b(\d{1,3}(?:\.\d{1,3}){3})\b/g)].map(m => {
    const ip = m[1];
    const type = isLocal(ip) ? "local" : "internet";
    return [type, ip];
  });

const reduce = async (key, values) => Array.from(new Set(values)); // deduplicate IPs
EOF

./target/release/pulsar -f /tmp/access.log -s script.js --output=json | jq
```

## Performance

```txt
$ ./benchmark.sh
Finished `release` profile [optimized] target(s) in 0.48s
    Finished `release` profile [optimized] target(s) in 0.16s
Benchmark 1: baseline-awk-20k-lines,pulsar-20k-lines,pulsar-sort-20k-lines
  Time (mean ± σ):     147.3 ms ±   1.5 ms    [User: 138.8 ms, System: 10.0 ms]
  Range (min … max):   145.2 ms … 150.2 ms    10 runs

Benchmark 2: cat input.txt | ./target/release/pulsar > /dev/null
  Time (mean ± σ):     312.9 ms ±   9.3 ms    [User: 1554.7 ms, System: 354.0 ms]
  Range (min … max):   296.1 ms … 332.3 ms    10 runs

Benchmark 3: cat input.txt | ./target/release/pulsar --sort > /dev/null
  Time (mean ± σ):     383.6 ms ±  10.1 ms    [User: 1641.2 ms, System: 349.9 ms]
  Range (min … max):   361.6 ms … 395.1 ms    10 runs

Summary
  baseline-awk-20k-lines,pulsar-20k-lines,pulsar-sort-20k-lines ran
    2.12 ± 0.07 times faster than cat input.txt | ./target/release/pulsar > /dev/null
    2.60 ± 0.07 times faster than cat input.txt | ./target/release/pulsar --sort > /dev/null
```

## Tests

```bash
bats tests
./benchmarks.sh # requires hyperfine
```
