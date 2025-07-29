#!/bin/bash

cargo rustc --release --bin pulsar -- -C debuginfo=2

perf record -g -F 999 --call-graph dwarf -- ./target/release/pulsar -f input.txt
perf script -F +pid > profile.perf