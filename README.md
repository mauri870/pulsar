# pulsar

`pulsar` is a high-performance MapReduce engine for processing large datasets using user-defined JavaScript functions. It follows the standard Unix philosophy, reads from stdin or a file, writes to stdout, and composes naturally with other tools via pipes.

Features include ES2023 JavaScript support via [Amazon AWS's LLRT](https://github.com/awslabs/llrt) engine based on [QuickJS](https://bellard.org/quickjs/), work-stealing scheduler, streaming input/output, automatically spills to disk if intermediate data is too large, NDJSON output, sorting and an embedded test runner.

Define `map`, `combine`, `reduce`, `sort`, and `test` as async functions in your script. The engine handles parallelism, chunking, grouping, and orchestration. In the diagrams below, lighter sections represent your script and darker sections represent the engine:

```mermaid
flowchart LR
  classDef engine fill:#000000,stroke:#fff,color:#fff
  subgraph pulsar["Pulsar"]
    direction TB
    pi([Input]) --> ch
    ch([Chunking]) --> sched
    subgraph sched["Scheduler"]
      direction LR
      subgraph w1["Thread 1 (JS VM)"]
        mc1["map / combine"]
      end
      subgraph w2["Thread 2 (JS VM)"]
        mc2["map / combine"]
      end
      subgraph wn["Thread N (JS VM)"]
        mcn["map / combine"]
      end
      mc1 ~~~ mc2 ~~~ mcn
    end
    sched --> group["Group"] --> reduce["reduce"] --> sort["Sort (optional)"] --> po([Output])
  end

  subgraph nodejs["NodeJS"]
    direction TB
    ni([Input]) --> evloop
    subgraph evloop["Event Loop (single thread)"]
      nmap["read, map, accumulate, reduce"]
    end
    evloop --> no([Output])
  end

  class sched engine
  class evloop engine
  class w1,w2,wn engine

  pulsar ~~~ nodejs

  classDef userCode fill:#d4edda,stroke:#28a745,color:#000
  class mc1,mc2,mcn,reduce,sort userCode
  class nmap userCode
```

## Compilation

Requires Rust, nvm, NodeJS.

```bash
./build_llrt.sh
cargo install --path=.
```

## Usage

By default, if no script is provided, it performs a simple word count. See [`default_script.js`](./default_script.js) for an example implementation.

```bash
pulsar -f input_file -s script_file
pulsar -h
```

## Examples

<details>
<summary>Open list of examples</summary>

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

</details>

## Performance

On latency-bound parallelizable workloads, `pulsar` is 1.3x faster than Node.js, see the benchmark bellow. 

<details>
<summary>perf.txt</summary>

```txt

NodeJS version: v25.7.0
Pulsar version: 0.1.0-bb2d25e
CPU: AMD Ryzen 9 9950X3D 16-Core Processor 32

Summary

This benchmark performs a simple word count aggregation on a 20,000-line
copy of the Moby Dick by Herman Melville.

Each line is processed by the map function, which introduces an artificial
delay with jitter (0.05-0.45 ms per line, uniform random) to simulate
variable processing times of a real workload.

It compares Pulsar against a NodeJS equivalent implementation. Both
versions are asynchronous but, due to the nature of NodeJS, it runs on a
single thread. Remember, concurrency is not parallelism.

Pulsar, on the other hand, is a highly parallel MapReduce engine and can
leverage multiple threads and multiple execution contexts.

   Compiling pulsar v0.1.0 (/home/mauri870/git/pulsar)
    Finished `release` profile [optimized] target(s) in 1.51s
Benchmark 1: pulsar-20k-lines
  Time (mean ± σ):      73.4 ms ±   4.3 ms    [User: 614.3 ms, System: 35.3 ms]
  Range (min … max):    69.5 ms …  79.9 ms    5 runs
 
Benchmark 2: pulsar-20k-lines-sort-by-key-asc
  Time (mean ± σ):     111.0 ms ±   3.5 ms    [User: 657.8 ms, System: 32.9 ms]
  Range (min … max):   106.7 ms … 116.2 ms    5 runs
 
Benchmark 3: baseline-node-20k-lines
  Time (mean ± σ):      90.3 ms ±   2.0 ms    [User: 107.0 ms, System: 18.9 ms]
  Range (min … max):    87.4 ms …  92.4 ms    5 runs
 
Summary
  pulsar-20k-lines ran
    1.23 ± 0.08 times faster than baseline-node-20k-lines
    1.51 ± 0.10 times faster than pulsar-20k-lines-sort-by-key-asc
Benchmark 1 (68 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          73.7ms ± 3.40ms    66.4ms … 80.9ms          0 ( 0%)        0%
  peak_rss            253MB ± 3.08MB     246MB …  259MB          3 ( 4%)        0%
  cpu_cycles         3.23G  ± 22.3M     3.17G  … 3.28G           1 ( 1%)        0%
  instructions       10.5G  ± 10.0M     10.5G  … 10.5G           2 ( 3%)        0%
  cache_references    128M  ±  942K      126M  …  131M           4 ( 6%)        0%
  cache_misses       5.83M  ± 77.7K     5.69M  … 6.03M           0 ( 0%)        0%
  branch_misses      5.30M  ± 37.4K     5.21M  … 5.39M           0 ( 0%)        0%
Benchmark 2 (46 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js --sort
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           109ms ± 3.62ms    98.4ms …  116ms          2 ( 4%)        💩+ 47.6% ±  1.8%
  peak_rss            265MB ± 5.02MB     255MB …  279MB          2 ( 4%)        💩+  4.9% ±  0.6%
  cpu_cycles         3.52G  ±  170M     3.35G  … 3.97G           3 ( 7%)        💩+  9.0% ±  1.3%
  instructions       11.3G  ±  144M     11.1G  … 11.6G           0 ( 0%)        💩+  8.0% ±  0.3%
  cache_references    131M  ± 1.81M      129M  …  138M           3 ( 7%)        💩+  2.1% ±  0.4%
  cache_misses       6.25M  ±  658K     5.81M  … 9.10M           9 (20%)        💩+  7.1% ±  2.7%
  branch_misses      6.04M  ±  227K     5.84M  … 6.91M           4 ( 9%)        💩+ 14.2% ±  1.0%
Benchmark 3 (56 runs): node node-script.js input.txt
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          89.9ms ± 4.23ms    84.5ms …  103ms          7 (13%)        💩+ 22.0% ±  1.8%
  peak_rss            102MB ±  691KB     100MB …  104MB          0 ( 0%)        ⚡- 59.7% ±  0.3%
  cpu_cycles          618M  ± 26.5M      590M  …  723M           4 ( 7%)        ⚡- 80.9% ±  0.3%
  instructions       1.46G  ± 21.6M     1.42G  … 1.54G           2 ( 4%)        ⚡- 86.1% ±  0.1%
  cache_references   53.8M  ±  965K     52.1M  … 56.2M           0 ( 0%)        ⚡- 58.1% ±  0.3%
  cache_misses       4.58M  ±  163K     4.40M  … 5.30M           2 ( 4%)        ⚡- 21.5% ±  0.8%
  branch_misses      4.50M  ±  209K     4.19M  … 5.05M           0 ( 0%)        ⚡- 15.0% ±  1.0%
```

</details>

## Tests

Each script can define a `async function test()` that will be executed when running the `pulsar` command with the `--test` flag.

For pulsar itself, there is an integration test suite that you can run with [bats](https://github.com/bats-core/bats-core):

```bash
bats tests
```
