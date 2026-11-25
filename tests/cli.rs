//! Characterization tests: they run the real binary and pin its observable
//! behavior (stdout, stderr, exit status) for the paths the bats suite does
//! not cover.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const BINARY: &str = env!("CARGO_BIN_EXE_pulsar");

/// A run that takes longer than this is reported as a failure instead of
/// blocking the whole test suite.
const RUN_TIMEOUT: Duration = Duration::from_secs(30);

struct Sandbox {
    directory: PathBuf,
}

impl Sandbox {
    fn new(name: &str) -> Self {
        let directory = std::env::temp_dir().join(format!("pulsar-cli-{}-{}", name, std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        Sandbox { directory }
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.directory.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    /// Runs the binary with `arguments`, feeding `stdin` to it.
    fn run(&self, arguments: &[&str], stdin: &str) -> Output {
        let mut child = Command::new(BINARY)
            .args(arguments)
            .current_dir(&self.directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(stdin.as_bytes()).unwrap();

        let deadline = Instant::now() + RUN_TIMEOUT;
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("pulsar {:?} did not finish within {:?}", arguments, RUN_TIMEOUT);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.wait_with_output().unwrap()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn stdout_lines(output: &Output) -> Vec<String> {
    let mut lines: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

const PASS_THROUGH: &str = "\
const map = async line => [[line, line.length]];
const reduce = async (key, values) => values[0];
";

#[test]
fn default_script_counts_words() {
    let sandbox = Sandbox::new("default-count");
    let output = sandbox.run(&[], "a b a\nb a\n");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), ["a: 3", "b: 2"]);
}

#[test]
fn empty_input_prints_nothing() {
    let sandbox = Sandbox::new("empty-input");
    let output = sandbox.run(&[], "");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}

#[test]
fn worker_count_does_not_change_the_result() {
    let sandbox = Sandbox::new("worker-count");
    let input = "x y z x\ny z x\nz\n";
    let expected = ["x: 3", "y: 2", "z: 3"];
    for arguments in [&["-j", "1"][..], &["-j", "3"], &["-c", "5"], &["-j", "2", "-c", "5"]] {
        let output = sandbox.run(arguments, input);
        assert_eq!(output.status.code(), Some(0), "arguments {:?}", arguments);
        assert_eq!(stdout_lines(&output), expected, "arguments {:?}", arguments);
    }
}

/// With many workers and a chunk size of 1 or 2, a run occasionally never
/// finishes: every VM thread ends up idle and the main task waits for map
/// workers that never complete. It shows up in a few percent of runs when
/// the machine is loaded, so this test starts many runs at once. It is
/// ignored by default because it fails until that hang is fixed.
/// Run it with `cargo test -- --ignored`.
#[test]
#[ignore = "known hang with small --chunk-size and many workers"]
fn small_chunk_size_with_many_workers_terminates() {
    let sandbox = Sandbox::new("small-chunk-hang");
    std::thread::scope(|scope| {
        for _ in 0..24 {
            scope.spawn(|| {
                for chunk_size in ["1", "2"] {
                    for _ in 0..5 {
                        let output = sandbox.run(&["-c", chunk_size], "x y z x\ny z x\nz\n");
                        assert_eq!(output.status.code(), Some(0));
                        assert_eq!(stdout_lines(&output), ["x: 3", "y: 2", "z: 3"]);
                    }
                }
            });
        }
    });
}

#[test]
fn json_output_renders_each_value_type() {
    let sandbox = Sandbox::new("json-types");
    let script = sandbox.write(
        "types.js",
        "\
const map = async line => [
  [\"string\", \"text\"],
  [\"integer\", 7],
  [\"float\", 1.5],
  [\"bool\", true],
  [\"null\", null],
  [\"array\", [1, \"two\"]],
  [\"object\", { nested: { n: 1 } }],
];
const reduce = async (key, values) => values[0];
",
    );
    let output = sandbox.run(&["-s", script.to_str().unwrap(), "--output=json"], "x\n");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        stdout_lines(&output),
        [
            r#"{"array":[1,"two"]}"#,
            r#"{"bool":true}"#,
            r#"{"float":1.5}"#,
            r#"{"integer":7}"#,
            r#"{"null":null}"#,
            r#"{"object":{"nested":{"n":1}}}"#,
            r#"{"string":"text"}"#,
        ]
    );
}

#[test]
fn plain_output_renders_each_value_type() {
    let sandbox = Sandbox::new("plain-types");
    let script = sandbox.write(
        "types.js",
        "\
const map = async line => [
  [\"string\", \"text\"],
  [\"integer\", 7],
  [\"float\", 1.5],
  [\"bool\", true],
  [\"null\", null],
  [\"array\", [1, \"two\"]],
  [\"object\", { a: 1 }],
];
const reduce = async (key, values) => values[0];
",
    );
    let output = sandbox.run(&["-s", script.to_str().unwrap()], "x\n");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        stdout_lines(&output),
        [
            "array: 1,two",
            "bool: true",
            "float: 1.5",
            "integer: 7",
            "null: null",
            "object: a: 1",
            "string: text",
        ]
    );
}

#[test]
fn combine_runs_before_reduce() {
    let sandbox = Sandbox::new("combine");
    let script = sandbox.write(
        "combine.js",
        "\
const map = async line => line.split(' ').map(word => [word, 1]);
const combine = async pairs => Object.entries(pairs.reduce((sums, [key, value]) => {
  sums[key] = (sums[key] || 0) + value;
  return sums;
}, {}));
const reduce = async (key, values) => values.reduce((sum, value) => sum + value, 0);
",
    );
    let output = sandbox.run(&["-s", script.to_str().unwrap()], "a a b\na b b\n");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), ["a: 3", "b: 3"]);
}

#[test]
fn missing_input_file_fails() {
    let sandbox = Sandbox::new("missing-input");
    let output = sandbox.run(&["-f", "does-not-exist.txt"], "");
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr_text(&output).contains("Failed to open file does-not-exist.txt"));
}

#[test]
fn missing_script_file_fails() {
    let sandbox = Sandbox::new("missing-script");
    let output = sandbox.run(&["-s", "does-not-exist.js"], "x\n");
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr_text(&output).contains("Failed to read script file does-not-exist.js"));
}

#[test]
fn unknown_flag_is_a_usage_error() {
    let sandbox = Sandbox::new("usage");
    let output = sandbox.run(&["--no-such-flag"], "");
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn syntax_error_exits_zero_with_no_output() {
    let sandbox = Sandbox::new("syntax-error");
    let script = sandbox.write("broken.js", "const map =\nvar reduce = (k, v) => v[0];\n");
    let output = sandbox.run(&["-s", script.to_str().unwrap()], "x\n");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
    assert!(stderr_text(&output).contains("Error loading JS code"));
}

#[test]
fn missing_map_function_exits_zero_with_no_output() {
    let sandbox = Sandbox::new("missing-map");
    let script = sandbox.write("nomap.js", "const reduce = async (k, v) => v[0];\n");
    let output = sandbox.run(&["-s", script.to_str().unwrap()], "x\n");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}

#[test]
fn missing_reduce_function_exits_zero_with_no_output() {
    let sandbox = Sandbox::new("missing-reduce");
    let script = sandbox.write("noreduce.js", "const map = async line => [[line, 1]];\n");
    let output = sandbox.run(&["-s", script.to_str().unwrap()], "x\n");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}

#[test]
fn throwing_reduce_drops_the_failed_batch_and_exits_zero() {
    let sandbox = Sandbox::new("reduce-throws");
    let script = sandbox.write(
        "throws.js",
        "\
const map = async line => [[line, 1]];
const reduce = async (key, values) => { throw new Error('boom'); };
",
    );
    let output = sandbox.run(&["-s", script.to_str().unwrap()], "a\nb\n");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
    assert!(stderr_text(&output).contains("boom"));
}

#[test]
fn throwing_map_exits_zero() {
    let sandbox = Sandbox::new("map-throws");
    let script = sandbox.write(
        "throws.js",
        "\
const map = async line => { throw new Error('map boom'); };
const reduce = async (key, values) => values[0];
",
    );
    let output = sandbox.run(&["-s", script.to_str().unwrap()], "a\n");
    assert_eq!(output.status.code(), Some(0));
    assert!(stderr_text(&output).contains("map boom"));
}

#[test]
fn sort_without_a_sort_function_prints_nothing() {
    let sandbox = Sandbox::new("sort-missing");
    let script = sandbox.write("nosort.js", PASS_THROUGH);
    let output = sandbox.run(&["-s", script.to_str().unwrap(), "--sort"], "ab\ncd\n");
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}

#[test]
fn sort_orders_the_output() {
    let sandbox = Sandbox::new("sort-order");
    let script = sandbox.write(
        "sort.js",
        "\
const map = async line => [[line, 0]];
const reduce = async (key, values) => values[0];
const sort = async results => results.sort((a, b) => a[0].localeCompare(b[0]));
",
    );
    let output = sandbox.run(&["-s", script.to_str().unwrap(), "--sort"], "c\na\nb\n");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(String::from_utf8_lossy(&output.stdout), "a: 0\nb: 0\nc: 0\n");
}

#[test]
fn test_mode_reports_ok_for_the_default_script() {
    let sandbox = Sandbox::new("test-mode");
    let output = sandbox.run(&["--test"], "");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(String::from_utf8_lossy(&output.stdout), "OK\n");
}

#[test]
fn input_without_trailing_newline_keeps_the_last_line() {
    let sandbox = Sandbox::new("no-trailing-newline");
    let output = sandbox.run(&[], "a\nb");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), ["a: 1", "b: 1"]);
}

#[test]
fn file_input_matches_stdin_input() {
    let sandbox = Sandbox::new("file-vs-stdin");
    sandbox.write("input.txt", "p q p\nq p\n");
    let from_file = sandbox.run(&["-f", "input.txt"], "");
    let from_stdin = sandbox.run(&[], "p q p\nq p\n");
    assert_eq!(from_file.status.code(), Some(0));
    assert_eq!(stdout_lines(&from_file), stdout_lines(&from_stdin));
    assert_eq!(stdout_lines(&from_file), ["p: 3", "q: 2"]);
}
