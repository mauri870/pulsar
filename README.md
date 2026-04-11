# pulsar

`pulsar` is a high-performance MapReduce engine for processing large datasets using user-defined JavaScript functions.

Features include parallel processing powered by Tokio, robust JavaScript support via [Amazon AWS's LLRT](https://github.com/awslabs/llrt) engine (based on [QuickJS](https://github.com/DelSkayn/rquickjs)), streaming, NDJSON output, and sorting.

By default, if no JS script is provided, it performs a simple word count. See `default_script.js` for the default behavior and available options.

## Compilation

```bash
./build_llrt.sh
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
NodeJS version: v25.7.0
Pulsar version: pulsar 0.1.0-8e99e45
CPU: AMD Ryzen 9 9950X3D 16-Core Processor 32

Summary

This benchmark performs a simple word count aggregation on a 20,000-line
copy of the Moby Dick by Herman Melville.

Each line is processed by the map function, which introduces an artificial
delay with jitter (0.05–0.45 ms per line, uniform random) to simulate
variable processing times.

It compares Pulsar against a NodeJS equivalent implementation. Both
versions are asynchronous but, due to the nature of NodeJS, it runs on a
single thread. Remember, concurrency is not parallelism.

Pulsar, on the other hand, is a highly parallel MapReduce engine and can
leverage multiple threads and multiple execution contexts.

    Finished `release` profile [optimized] target(s) in 0.09s
Benchmark 1: pulsar-20k-lines
  Time (mean ± σ):     130.7 ms ±   6.5 ms    [User: 675.0 ms, System: 44.0 ms]
  Range (min … max):   121.1 ms … 136.9 ms    5 runs
 
Benchmark 2: pulsar-20k-lines-sort-by-key-asc
  Time (mean ± σ):     167.6 ms ±   8.3 ms    [User: 755.3 ms, System: 48.0 ms]
  Range (min … max):   159.0 ms … 177.5 ms    5 runs
 
Benchmark 3: baseline-node-20k-lines
  Time (mean ± σ):      5.604 s ±  0.029 s    [User: 5.230 s, System: 0.436 s]
  Range (min … max):    5.574 s …  5.635 s    5 runs
 
Summary
  pulsar-20k-lines ran
    1.28 ± 0.09 times faster than pulsar-20k-lines-sort-by-key-asc
   42.89 ± 2.13 times faster than baseline-node-20k-lines
Benchmark 1 (38 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           132ms ± 7.71ms     115ms …  144ms          4 (11%)        0%
  peak_rss            349MB ± 3.90MB     336MB …  358MB          1 ( 3%)        0%
  cpu_cycles         3.64G  ± 58.0M     3.52G  … 3.86G           4 (11%)        0%
  instructions       11.1G  ± 59.4M     11.1G  … 11.5G           4 (11%)        0%
  cache_references    157M  ± 1.26M      155M  …  160M           0 ( 0%)        0%
  cache_misses       10.7M  ±  181K     10.2M  … 11.0M           0 ( 0%)        0%
  branch_misses      7.98M  ±  112K     7.83M  … 8.48M           2 ( 5%)        0%
Benchmark 2 (31 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js --sort
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           164ms ± 8.43ms     148ms …  176ms          0 ( 0%)        💩+ 23.7% ±  2.9%
  peak_rss            362MB ± 4.13MB     354MB …  370MB          0 ( 0%)        💩+  3.9% ±  0.6%
  cpu_cycles         4.02G  ±  144M     3.76G  … 4.27G           0 ( 0%)        💩+ 10.5% ±  1.4%
  instructions       12.0G  ±  121M     11.7G  … 12.2G           0 ( 0%)        💩+  7.6% ±  0.4%
  cache_references    160M  ± 2.05M      157M  …  164M           0 ( 0%)        💩+  1.9% ±  0.5%
  cache_misses       11.2M  ±  689K     10.3M  … 12.9M           0 ( 0%)        💩+  5.1% ±  2.2%
  branch_misses      8.66M  ±  188K     8.31M  … 9.08M           0 ( 0%)        💩+  8.4% ±  0.9%
Benchmark 3 (3 runs): node node-script.js input.txt
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          5.59s  ± 17.2ms    5.57s  … 5.60s           0 ( 0%)        💩+4123.8% ±  7.8%
  peak_rss           81.5MB ±  814KB    80.6MB … 82.3MB          0 ( 0%)        ⚡- 76.7% ±  1.3%
  cpu_cycles         27.8G  ±  218M     27.5G  … 27.9G           0 ( 0%)        💩+662.8% ±  2.5%
  instructions       87.6G  ±  989M     86.8G  … 88.7G           0 ( 0%)        💩+686.5% ±  2.5%
  cache_references   7.35G  ±  127M     7.20G  … 7.42G           0 ( 0%)        💩+4574.5% ± 22.4%
  cache_misses       18.2M  ±  300K     17.9M  … 18.5M           0 ( 0%)        💩+ 70.6% ±  2.2%
  branch_misses      16.1M  ± 71.8K     16.0M  … 16.1M           0 ( 0%)        💩+101.2% ±  1.7%
```

</details>

## Tests

Each script can define a `async function test()` that will be executed when running the `pulsar` command with the `--test` flag.

For pulsar itself, there is a very modest test suite that you can run with [bats](https://github.com/bats-core/bats-core):

```bash
bats tests
```
