// Copyright 2026 houseme
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CLI entry point of the `thinkingdata` binary.
//!
//! Parsing errors and SDK errors are reported on stderr with distinct exit
//! codes, so the CLI composes cleanly in scripts and CI pipelines.

use clap::Parser;

use thinkingdata::cli::{Cli, run};

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli) {
        eprintln!("[ThinkingData][CLI] error: {err}");
        std::process::exit(1);
    }
    println!("OK");
}
