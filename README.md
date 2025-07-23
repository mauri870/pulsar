# mapreduce

`mapreduce` is a command-line tool that processes input data by line using user-defined map and reduce functions written in JavaScript. It is designed for high parallelism and scales well on large input streams.

By default, if no script is specified, it performs a simple word count.

## Compilation

```bash
cargo build --release
```

The binary will be located at ./target/release/mapreduce.

## Usage

```bash
./target/release/mapreduce -f input_file -s script_file
```

## Examples

### Word Count (Default)
Counting the words in a text file:

```bash
# download Moby Dick from Gutenberg
wget https://www.gutenberg.org/files/2701/2701-0.txt -O input.txt

# default script does word counting, check default_script.js
time ./target/release/mapreduce -f input.txt | sort -t':' -k2 -n
...
a: 4747
and: 6447
of: 6626
the: 14537

real	0m0.980s
user	0m26.396s
sys	    0m0.203s
```

You could extend it to ignore stop words:

```bash
cat > script.js << 'EOF'
const STOP_WORDS = new Set([
  "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
  "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
  "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"
]);

const map = line => line
  .toLowerCase()
  .replace(/[^\p{L}\p{N}]+/gu, ' ')
  .trim()
  .split(/\s+/)
  .filter(word => word && !STOP_WORDS.has(word))
  .map(word => [word, 1]);

const reduce = (key, values) => values.length;
EOF

./target/release/mapreduce -f input.txt -s script.js
```

### Log Analysis
Analyze web server logs to count status codes:

```bash
# generate logs
docker run --rm mingrammer/flog -n 1000 > /tmp/access.log

cat > script.js << 'EOF'
const map = line => {
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

const reduce = (key, values) =>
  values.reduce((sum, count) => sum + count, 0);
EOF

./target/release/mapreduce -f /tmp/access.log -s script.js
```

You could build on this to aggregate by local vs internet IPs:

```bash
cat > script.js << 'EOF'
const isLocal = ip => {
  const [a, b] = ip.split('.').map(Number);
  return a === 10 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || a === 127;
};

const map = line => [...line.matchAll(/\b(\d{1,3}(?:\.\d{1,3}){3})\b/g)]
  .map(m => isLocal(m[1]) ? ["local", 1] : ["internet", 1]);

const reduce = (key, values) => values.reduce((a, b) => a + b, 0);
EOF

./target/release/mapreduce -f /tmp/access.log -s script.js | sort -rn -t':' -k2
```

## Tests

```bash
bats tests
```