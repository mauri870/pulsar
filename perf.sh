#!/bin/bash

set -e

cargo b --release
perf record -g -F 999 -- ./target/release/pulsar -f input.txt 
perf script -F +pid > profile.perf