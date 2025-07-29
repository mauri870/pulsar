# pulsar

`pulsar` is a high-performance MapReduce engine for processing large datasets using user-defined JavaScript functions.

Features include parallel processing powered by Tokio, robust JavaScript support via [Amazon AWS's LLRT](https://github.com/awslabs/llrt) engine (based on [QuickJS](https://github.com/DelSkayn/rquickjs)), streaming, NDJSON output, and sorting.

By default, if no JS script is provided, it performs a simple word count. See `default_script.js` for the default behavior and available options.

## Compilation

```bash
nvm use 22
(cd llrt && npm i && make js)
cargo install --path=.
```

## Usage

```bash
pulsar -f input_file -s script_file
```

## Examples

### Word Count (Default)

Counting the words in a text file:

```bash
# download Moby Dick from Gutenberg
$ wget https://www.gutenberg.org/files/2701/2701-0.txt -O input.txt
$ wc -l input.txt
21940

$ cat input.txt | pulsar
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

const map = async (line) =>
  line
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .split(/\s+/)
    .filter(
      (word) => word && !STOP_WORDS.has(word) && !/\d/.test(word) // filter out any word containing digits
    )
    .map((word) => [word, 1]);

const reduce = async (key, values) => values.length;

const sort = async (results) =>
  results.sort((a, b) => a[0].localeCompare(b[0])); // Sort alphabetically
```

</details>

```bash
$ pulsar -f input.txt -s script.js --sort | head -n5
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
const map = async (line) => {
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
$ pulsar -f /tmp/access.log -s script.js
501: 47
416: 50
404: 43
204: 50
```

You could build on this to aggregate local vs internet IPs, then print the results in json:

<details>
<summary>script.js</summary>

```js
const isLocal = (ip) => {
  const [a, b] = ip.split(".").map(Number);
  return (
    a === 10 ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168) ||
    a === 127
  );
};

const map = async (line) =>
  [...line.matchAll(/\b(\d{1,3}(?:\.\d{1,3}){3})\b/g)].map((m) => {
    const ip = m[1];
    const type = isLocal(ip) ? "local" : "internet";
    return [type, ip];
  });

const reduce = async (key, values) => Array.from(new Set(values)); // deduplicate IPs
```

</details>

```bash
$ pulsar -f /tmp/access.log -s script.js --output=json | jq
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
$ socat TCP-LISTEN:1234,reuseaddr,fork EXEC:"pulsar -s script.js --output=json" &
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
Pulsar version: pulsar 0.1.0-b2cb996
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

    Finished `release` profile [optimized] target(s) in 0.19s
Benchmark 1: pulsar-20k-lines
  Time (mean ± σ):     154.0 ms ±   8.6 ms    [User: 1475.8 ms, System: 110.7 ms]
  Range (min … max):   144.2 ms … 166.7 ms    5 runs

Benchmark 2: pulsar-20k-lines-sort-by-key-asc
  Time (mean ± σ):     223.9 ms ±   5.8 ms    [User: 1559.3 ms, System: 113.3 ms]
  Range (min … max):   214.7 ms … 229.6 ms    5 runs

Benchmark 3: baseline-node-20k-lines
  Time (mean ± σ):      5.282 s ±  0.004 s    [User: 4.374 s, System: 1.113 s]
  Range (min … max):    5.275 s …  5.287 s    5 runs

Summary
  pulsar-20k-lines ran
    1.45 ± 0.09 times faster than pulsar-20k-lines-sort-by-key-asc
   34.30 ± 1.91 times faster than baseline-node-20k-lines
Benchmark 1 (31 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           163ms ± 6.58ms     150ms …  177ms          0 ( 0%)        0%
  peak_rss           48.7MB ±  420KB    48.0MB … 49.5MB          0 ( 0%)        0%
  cpu_cycles         6.38G  ± 60.1M     6.29G  … 6.52G           0 ( 0%)        0%
  instructions       11.7G  ± 2.73M     11.7G  … 11.7G           0 ( 0%)        0%
  cache_references    242M  ± 2.76M      238M  …  248M           0 ( 0%)        0%
  cache_misses       30.8M  ±  516K     29.7M  … 31.9M           0 ( 0%)        0%
  branch_misses      14.6M  ±  307K     14.3M  … 16.1M           1 ( 3%)        0%
Benchmark 2 (3 runs): node node-script.js input.txt
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          5.29s  ± 12.1ms    5.28s  … 5.31s           0 ( 0%)        💩+3138.4% ±  5.3%
  peak_rss           75.8MB ±  368KB    75.6MB … 76.3MB          0 ( 0%)        💩+ 55.6% ±  1.1%
  cpu_cycles         17.4G  ±  114M     17.4G  … 17.6G           0 ( 0%)        💩+173.3% ±  1.3%
  instructions       35.1G  ±  212M     34.9G  … 35.3G           0 ( 0%)        💩+200.3% ±  0.6%
  cache_references   2.79G  ± 28.8M     2.77G  … 2.83G           0 ( 0%)        💩+1054.5% ±  3.9%
  cache_misses       54.2M  ±  502K     53.6M  … 54.6M           0 ( 0%)        💩+ 76.1% ±  2.1%
  branch_misses      27.5M  ±  156K     27.3M  … 27.6M           0 ( 0%)        💩+ 88.3% ±  2.5%
```

</details>

## Tests

Each script can define a `async function test()` that will be executed when running the `pulsar` command with the `--test` flag.

For pulsar itself, there is a very modest test suite that you can run with [bats](https://github.com/bats-core/bats-core):

```bash
bats tests
```
