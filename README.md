# pulsar

`pulsar` is a high-performance MapReduce engine for processing large datasets using user-defined JavaScript functions.

Features include parallel processing powered by Tokio, robust JavaScript support via [Amazon AWS's LLRT](https://github.com/awslabs/llrt) engine (based on [QuickJS](https://github.com/DelSkayn/rquickjs)), streaming, NDJSON output, and sorting.

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

<details>
<summary>script.js</summary>

```js
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
  .filter(word => 
    word && 
    !STOP_WORDS.has(word) &&
    !/\d/.test(word)    // filter out any word containing digits
  )
  .map(word => [word, 1]);

const reduce = async (key, values) => values.length;

const sort = async results =>
  results.sort((a, b) => a[0].localeCompare(b[0])); // Sort alphabetically
```
</details>

```bash
$ ./target/release/pulsar -f input.txt -s script.js --sort | head -n5
aback: 2
abaft: 2
abandon: 3
abandoned: 7
abandonedly: 1
```

### Log Analysis

Summarize web server logs to count logs per status codes:

<details>
<summary>script.js</summary>

```js
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
```
</details>

```bash
docker run --rm mingrammer/flog -n 1000 >> /tmp/access.log
$ ./target/release/pulsar -f /tmp/access.log -s script.js
501: 47
416: 50
404: 43
204: 50
```

You could build on this to aggregate local vs internet IPs, then print the results in json:

<details>
<summary>script.js</summary>

```js
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
```
</details>

```bash
$ ./target/release/pulsar -f /tmp/access.log -s script.js --output=json | jq
{
  "local": [
    "172.22.38.139",
    "127.45.14.34",
  ]
}
{
  "internet": [
    "237.253.60.152",
  ]
}
```

If you want to go further, we can turn this into a simple network server:

```bash
$ socat TCP-LISTEN:1234,reuseaddr,fork EXEC:"./target/release/pulsar -s script.js --output=json" &
$ echo "138.97.172.41 - - [26/Jul/2025:17:27:15 +0000] "PATCH /matrix/morph HTTP/1.0" 401 9375" | socat - TCP:localhost:1234
{"internet":["138.97.172.41"]}
$ killall socat
```

Not very efficient, but you get the idea.

## Performance

<details>
<summary>perf.txt</summary>

```txt
NodeJS version: v22.16.0
Pulsar version: pulsar 0.1.0-80765e5
CPU: Intel(R) Xeon(R) CPU E5-2697A v4 @ 2.60GHz 32

Summary

This benchmark performs a simple word count aggregation on a 20,000-line
copy of the Moby Dick by Herman Melville.

Each line is processed by the map function, which introduces an artificial
delay of approximately 0.23 ms per line, to simulate processing.

It compares Pulsar against a NodeJS equivalent implementation. Both
versions are asynchronous but, due to the nature of NodeJS, it runs on a
single thread. Remember, concurrency is not parallelism.

Pulsar, on the other hand, is a highly parallel MapReduce engine and can
leverage multiple threads and multiple execution contexts.

    Finished `release` profile [optimized] target(s) in 0.19s
Benchmark 1: pulsar-20k-lines
  Time (mean ± σ):     399.1 ms ±   7.9 ms    [User: 4700.2 ms, System: 3474.2 ms]
  Range (min … max):   388.5 ms … 410.4 ms    5 runs
 
Benchmark 2: pulsar-20k-lines-sort-by-key-asc
  Time (mean ± σ):     521.3 ms ±   8.0 ms    [User: 4886.5 ms, System: 3347.4 ms]
  Range (min … max):   507.4 ms … 526.5 ms    5 runs
 
Benchmark 3: baseline-node-20k-lines
  Time (mean ± σ):      5.347 s ±  0.007 s    [User: 5.144 s, System: 0.331 s]
  Range (min … max):    5.338 s …  5.359 s    5 runs
 
Summary
  pulsar-20k-lines ran
    1.31 ± 0.03 times faster than pulsar-20k-lines-sort-by-key-asc
   13.40 ± 0.27 times faster than baseline-node-20k-lines
Benchmark 1 (13 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           404ms ± 4.89ms     397ms …  414ms          0 ( 0%)        0%
  peak_rss           57.0MB ±  541KB    56.4MB … 57.9MB          0 ( 0%)        0%
  cpu_cycles         14.5G  ±  119M     14.3G  … 14.7G           0 ( 0%)        0%
  instructions       13.7G  ± 87.4M     13.5G  … 13.8G           0 ( 0%)        0%
  cache_references    167M  ± 3.30M      162M  …  173M           2 (15%)        0%
  cache_misses        514K  ± 57.2K      464K  …  607K           0 ( 0%)        0%
  branch_misses      29.2M  ±  503K     28.5M  … 29.9M           0 ( 0%)        0%
Benchmark 2 (3 runs): node node-script.js input.txt
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          5.34s  ± 1.95ms    5.34s  … 5.35s           0 ( 0%)        💩+1224.3% ±  1.6%
  peak_rss           80.8MB ±  572KB    80.2MB … 81.4MB          0 ( 0%)        💩+ 41.7% ±  1.3%
  cpu_cycles         16.7G  ± 42.0M     16.7G  … 16.7G           0 ( 0%)        💩+ 15.3% ±  1.1%
  instructions       30.3G  ±  274M     30.1G  … 30.6G           0 ( 0%)        💩+121.6% ±  1.3%
  cache_references   94.9M  ± 1.03M     93.8M  … 95.9M           0 ( 0%)        ⚡- 43.1% ±  2.5%
  cache_misses        433K  ± 43.2K      400K  …  482K           0 ( 0%)          - 15.8% ± 14.8%
  branch_misses      22.4M  ±  320K     22.0M  … 22.6M           0 ( 0%)        ⚡- 23.4% ±  2.3%
```
</details>

## Tests

```bash
bats tests
```
