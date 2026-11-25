//! Runs the shared conformance tests against the llrt runtime.

use crate::error::{Error, Result};
use crate::runtime::ScriptRuntime;
use crate::runtime::conformance::conformance_tests;
use crate::script::Contract;
use crate::value::{KeyValue, Value};
use std::collections::HashMap;

type Runtime = super::LlrtRuntime;

conformance_tests!();
