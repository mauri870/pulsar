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

$ cat input.txt | ./target/release/pulsar
...
bluish: 2
pedlar: 1
magazine: 2
reckless: 5
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
NodeJS version: v22.17.1
Pulsar version: pulsar 0.1.0-376e61a
CPU: AMD Ryzen 7 5800X3D 8-Core Processor 16

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

    Finished `release` profile [optimized] target(s) in 0.16s
Benchmark 1: pulsar-20k-lines
  Time (mean ± σ):     164.3 ms ±   8.7 ms    [User: 1619.3 ms, System: 120.6 ms]
  Range (min … max):   153.3 ms … 172.6 ms    5 runs

Benchmark 2: pulsar-20k-lines-sort-by-key-asc
  Time (mean ± σ):     239.2 ms ±   7.1 ms    [User: 1694.9 ms, System: 138.2 ms]
  Range (min … max):   231.1 ms … 249.9 ms    5 runs

Benchmark 3: baseline-node-20k-lines
  Time (mean ± σ):      5.306 s ±  0.004 s    [User: 4.264 s, System: 1.146 s]
  Range (min … max):    5.302 s …  5.310 s    5 runs

Summary
  pulsar-20k-lines ran
    1.46 ± 0.09 times faster than pulsar-20k-lines-sort-by-key-asc
   32.29 ± 1.71 times faster than baseline-node-20k-lines
Benchmark 1 (30 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           167ms ± 9.38ms     153ms …  186ms          0 ( 0%)        0%
  peak_rss           48.7MB ±  494KB    47.6MB … 49.8MB          0 ( 0%)        0%
  cpu_cycles         6.46G  ±  132M     6.26G  … 6.80G           0 ( 0%)        0%
  instructions       11.7G  ± 3.00M     11.7G  … 11.7G           0 ( 0%)        0%
  cache_references    250M  ± 7.42M      240M  …  271M           0 ( 0%)        0%
  cache_misses       32.9M  ± 2.13M     30.6M  … 38.5M           3 (10%)        0%
  branch_misses      15.0M  ±  523K     14.4M  … 16.4M           3 (10%)        0%
Benchmark 2 (3 runs): node node-script.js input.txt
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          5.32s  ± 1.22ms    5.32s  … 5.32s           0 ( 0%)        💩+3091.3% ±  6.7%
  peak_rss           76.1MB ±  398KB    75.7MB … 76.5MB          0 ( 0%)        💩+ 56.1% ±  1.2%
  cpu_cycles         17.2G  ± 49.1M     17.1G  … 17.2G           0 ( 0%)        💩+165.7% ±  2.5%
  instructions       34.4G  ±  341M     34.1G  … 34.8G           0 ( 0%)        💩+194.2% ±  0.9%
  cache_references   2.78G  ± 36.7M     2.74G  … 2.81G           0 ( 0%)        💩+1011.8% ±  5.8%
  cache_misses       54.7M  ±  583K     54.0M  … 55.1M           0 ( 0%)        💩+ 66.1% ±  7.8%
  branch_misses      28.5M  ± 2.14M     27.2M  … 31.0M           0 ( 0%)        💩+ 89.5% ±  6.1%
```
</details>

## Tests

```bash
bats tests
```
