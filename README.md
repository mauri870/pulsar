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
NodeJS version: v22.17.1
Pulsar version: pulsar 0.1.0-aef0ddb
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
  Time (mean ± σ):     168.9 ms ±   9.6 ms    [User: 1695.0 ms, System: 141.3 ms]
  Range (min … max):   155.5 ms … 181.3 ms    5 runs

Benchmark 2: pulsar-20k-lines-sort-by-key-asc
  Time (mean ± σ):     239.6 ms ±  17.7 ms    [User: 1664.8 ms, System: 161.6 ms]
  Range (min … max):   226.0 ms … 270.5 ms    5 runs

Benchmark 3: baseline-node-20k-lines
  Time (mean ± σ):      5.316 s ±  0.020 s    [User: 4.283 s, System: 1.142 s]
  Range (min … max):    5.290 s …  5.345 s    5 runs

Summary
  pulsar-20k-lines ran
    1.42 ± 0.13 times faster than pulsar-20k-lines-sort-by-key-asc
   31.47 ± 1.79 times faster than baseline-node-20k-lines
Benchmark 1 (26 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           193ms ± 19.1ms     169ms …  272ms          1 ( 4%)        0%
  peak_rss           44.9MB ±  329KB    44.2MB … 45.7MB          0 ( 0%)        0%
  cpu_cycles         6.87G  ±  152M     6.65G  … 7.38G           3 (12%)        0%
  instructions       11.7G  ± 15.0M     11.7G  … 11.8G           0 ( 0%)        0%
  cache_references    269M  ± 7.82M      258M  …  295M           1 ( 4%)        0%
  cache_misses       39.4M  ± 2.48M     37.2M  … 49.1M           2 ( 8%)        0%
  branch_misses      16.4M  ±  628K     15.7M  … 18.8M           1 ( 4%)        0%
Benchmark 2 (3 runs): node node-script.js input.txt
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          5.32s  ± 9.85ms    5.31s  … 5.33s           0 ( 0%)        💩+2664.1% ± 12.1%
  peak_rss           75.3MB ±  485KB    74.9MB … 75.8MB          0 ( 0%)        💩+ 67.8% ±  1.0%
  cpu_cycles         17.1G  ±  128M     16.9G  … 17.2G           0 ( 0%)        💩+148.3% ±  2.7%
  instructions       33.5G  ±  515M     33.0G  … 33.9G           0 ( 0%)        💩+185.8% ±  1.5%
  cache_references   2.75G  ± 80.2M     2.66G  … 2.82G           0 ( 0%)        💩+921.0% ± 10.7%
  cache_misses       56.6M  ± 2.57M     54.5M  … 59.5M           0 ( 0%)        💩+ 43.5% ±  7.9%
  branch_misses      27.0M  ±  390K     26.6M  … 27.3M           0 ( 0%)        💩+ 64.4% ±  4.7%
```
</details>

## Tests

```bash
bats tests
```
