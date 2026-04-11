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

On latency-bound parallelizable workloads, `pulsar` is upwards of 75x faster than Node.js, the benchmark bellow. On more CPU intensive tasks, it can be up to 1.5x faster, but will use much more memory and cpu for the same task.

<details>
<summary>perf.txt</summary>

```txt

NodeJS version: v25.7.0
Pulsar version: 0.1.0-ea67cb8
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

    Finished `release` profile [optimized] target(s) in 0.07s
Benchmark 1: pulsar-20k-lines
  Time (mean ± σ):      72.0 ms ±   2.3 ms    [User: 616.9 ms, System: 33.7 ms]
  Range (min … max):    69.1 ms …  74.7 ms    5 runs
 
Benchmark 2: pulsar-20k-lines-sort-by-key-asc
  Time (mean ± σ):     111.3 ms ±   4.4 ms    [User: 652.5 ms, System: 30.0 ms]
  Range (min … max):   104.8 ms … 116.2 ms    5 runs
 
Benchmark 3: baseline-node-20k-lines
  Time (mean ± σ):      5.567 s ±  0.014 s    [User: 5.172 s, System: 0.462 s]
  Range (min … max):    5.546 s …  5.580 s    5 runs
 
Summary
  pulsar-20k-lines ran
    1.55 ± 0.08 times faster than pulsar-20k-lines-sort-by-key-asc
   77.34 ± 2.48 times faster than baseline-node-20k-lines
Benchmark 1 (70 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          71.6ms ± 4.07ms    61.4ms … 80.6ms          0 ( 0%)        0%
  peak_rss            253MB ± 2.21MB     247MB …  257MB          1 ( 1%)        0%
  cpu_cycles         3.26G  ± 28.7M     3.21G  … 3.36G           1 ( 1%)        0%
  instructions       10.5G  ± 9.42M     10.5G  … 10.5G           0 ( 0%)        0%
  cache_references    129M  ± 1.47M      125M  …  135M           1 ( 1%)        0%
  cache_misses       6.24M  ±  103K     6.02M  … 6.50M           1 ( 1%)        0%
  branch_misses      5.35M  ± 39.1K     5.26M  … 5.44M           0 ( 0%)        0%
Benchmark 2 (46 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js --sort
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           110ms ± 3.77ms     100ms …  119ms          1 ( 2%)        💩+ 54.1% ±  2.1%
  peak_rss            267MB ± 6.49MB     256MB …  285MB          5 (11%)        💩+  5.5% ±  0.7%
  cpu_cycles         3.57G  ±  199M     3.36G  … 4.03G           0 ( 0%)        💩+  9.7% ±  1.5%
  instructions       11.3G  ±  169M     11.1G  … 11.7G           0 ( 0%)        💩+  8.0% ±  0.4%
  cache_references    133M  ± 1.95M      130M  …  139M           3 ( 7%)        💩+  2.7% ±  0.5%
  cache_misses       6.87M  ±  819K     6.27M  … 9.64M           2 ( 4%)        💩+ 10.2% ±  3.1%
  branch_misses      6.15M  ±  278K     5.87M  … 7.22M           1 ( 2%)        💩+ 14.9% ±  1.2%
Benchmark 3 (3 runs): node node-script.js input.txt
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          5.59s  ± 18.2ms    5.57s  … 5.61s           0 ( 0%)        💩+7706.7% ±  8.3%
  peak_rss           76.1MB ± 10.5MB    64.1MB … 83.0MB          0 ( 0%)        ⚡- 69.9% ±  1.3%
  cpu_cycles         27.3G  ±  147M     27.1G  … 27.4G           0 ( 0%)        💩+738.8% ±  1.4%
  instructions       87.5G  ±  346M     87.1G  … 87.8G           0 ( 0%)        💩+734.7% ±  0.7%
  cache_references   7.32G  ±  103M     7.20G  … 7.39G           0 ( 0%)        💩+5566.8% ± 15.8%
  cache_misses       25.5M  ± 7.46M     17.1M  … 31.4M           0 ( 0%)        💩+308.0% ± 23.7%
  branch_misses      16.3M  ±  346K     15.9M  … 16.6M           0 ( 0%)        💩+204.5% ±  1.5%
```

</details>

## Tests

Each script can define a `async function test()` that will be executed when running the `pulsar` command with the `--test` flag.

For pulsar itself, there is an integration test suite that you can run with [bats](https://github.com/bats-core/bats-core):

```bash
bats tests
```
