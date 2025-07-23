# mapreduce

`mapreduce` is a command-line tool that processes input data using user-defined map and reduce functions written in JavaScript. It supports high parallelism for efficient processing of large datasets.

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

- Counting the words in a text file:

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

## Tests

```bash
bats tests
```