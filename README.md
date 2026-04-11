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

On CPU-bound parallelizable workloads, `pulsar` is upwards of 43x faster than Node.js, see [perf](#performance) section. By default, if no script is provided, it performs a simple word count. See [`default_script.js`](./default_script.js) for an example implementation.

## Compilation

Requires Rust, nvm, NodeJS.

```bash
./build_llrt.sh
cargo install --path=.
```

## Usage

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

<details>
<summary>perf.txt</summary>

```txt
NodeJS version: v25.7.0
Pulsar version: pulsar 0.1.0-3c74d3e
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

    Finished `release` profile [optimized] target(s) in 0.07s
Benchmark 1: pulsar-20k-lines
  Time (mean ± σ):     128.9 ms ±   9.7 ms    [User: 681.2 ms, System: 52.9 ms]
  Range (min … max):   119.2 ms … 141.0 ms    5 runs
 
Benchmark 2: pulsar-20k-lines-sort-by-key-asc
  Time (mean ± σ):     163.0 ms ±   5.0 ms    [User: 777.6 ms, System: 53.5 ms]
  Range (min … max):   159.5 ms … 171.4 ms    5 runs
 
Benchmark 3: baseline-node-20k-lines
  Time (mean ± σ):      5.607 s ±  0.017 s    [User: 5.194 s, System: 0.479 s]
  Range (min … max):    5.584 s …  5.627 s    5 runs
 
Summary
  pulsar-20k-lines ran
    1.26 ± 0.10 times faster than pulsar-20k-lines-sort-by-key-asc
   43.49 ± 3.29 times faster than baseline-node-20k-lines
Benchmark 1 (38 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           132ms ± 9.21ms                               116ms …  149ms                                    0 ( 0%)        0%
  peak_rss            109MB ± 2.73MB                               104MB …  115MB                                    0 ( 0%)        0%
  cpu_cycles         3.66G  ± 69.9M                               3.55G  … 3.89G                                     1 ( 3%)        0%
  instructions       11.2G  ± 58.0M                               11.1G  … 11.3G                                     7 (18%)        0%
  cache_references    158M  ± 1.83M                                154M  …  161M                                     0 ( 0%)        0%
  cache_misses       11.0M  ±  286K                               10.5M  … 11.5M                                     0 ( 0%)        0%
  branch_misses      8.14M  ± 82.6K                               7.98M  … 8.35M                                     0 ( 0%)        0%
Benchmark 2 (30 runs): ./target/release/pulsar -f input.txt -s pulsar-script.js --sort
  measurement          mean ± σ            min … max           outliers         delta
  wall_time           170ms ± 8.59ms                               158ms …  189ms                                    0 ( 0%)        💩+ 28.5% ±  3.3%
  peak_rss            124MB ± 7.12MB                               116MB …  156MB                                    1 ( 3%)        💩+ 13.5% ±  2.3%
  cpu_cycles         4.09G  ±  164M                               3.74G  … 4.36G                                     0 ( 0%)        💩+ 11.7% ±  1.6%
  instructions       12.0G  ±  156M                               11.7G  … 12.3G                                     3 (10%)        💩+  7.9% ±  0.5%
  cache_references    162M  ± 2.10M                                158M  …  168M                                     1 ( 3%)        💩+  2.9% ±  0.6%
  cache_misses       11.7M  ±  804K                               10.7M  … 13.2M                                     0 ( 0%)        💩+  6.2% ±  2.5%
  branch_misses      8.87M  ±  217K                               8.51M  … 9.45M                                     0 ( 0%)        💩+  9.1% ±  0.9%
Benchmark 3 (3 runs): node node-script.js input.txt
  measurement          mean ± σ            min … max           outliers         delta
  wall_time          5.59s  ± 4.82ms                              5.58s  … 5.59s                                     0 ( 0%)        💩+4124.2% ±  8.4%
  peak_rss           79.8MB ± 98.6KB                              79.7MB … 79.8MB                                    0 ( 0%)        ⚡- 27.0% ±  3.0%
  cpu_cycles         27.7G  ±  226M                               27.4G  … 27.8G                                     0 ( 0%)        💩+655.5% ±  2.8%
  instructions       85.8G  ±  939M                               84.9G  … 86.7G                                     0 ( 0%)        💩+669.1% ±  2.4%
  cache_references   7.16G  ±  119M                               7.03G  … 7.26G                                     0 ( 0%)        💩+4437.9% ± 21.0%
  cache_misses       22.9M  ± 7.72M                               18.0M  … 31.8M                                     0 ( 0%)        💩+107.7% ± 19.7%
  branch_misses      16.0M  ±  192K                               15.8M  … 16.2M                                     0 ( 0%)        💩+ 97.2% ±  1.4%
```

</details>

## Tests

Each script can define a `async function test()` that will be executed when running the `pulsar` command with the `--test` flag.

For pulsar itself, there is an integration test suite that you can run with [bats](https://github.com/bats-core/bats-core):

```bash
bats tests
```
