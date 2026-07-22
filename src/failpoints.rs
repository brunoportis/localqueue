//! Deterministic transaction hooks compiled only for the crash harness.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failpoint {
    EnqueueAfterBegin,
    EnqueueBeforeCommit,
    ClaimBeforeCommit,
    AckBeforeCommit,
    NackBeforeCommit,
    FailBeforeCommit,
}

impl Failpoint {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "enqueue-after-begin" => Self::EnqueueAfterBegin,
            "enqueue-before-commit" => Self::EnqueueBeforeCommit,
            "claim-before-commit" => Self::ClaimBeforeCommit,
            "ack-before-commit" => Self::AckBeforeCommit,
            "nack-before-commit" => Self::NackBeforeCommit,
            "fail-before-commit" => Self::FailBeforeCommit,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        match self {
            Self::EnqueueAfterBegin => "enqueue-after-begin",
            Self::EnqueueBeforeCommit => "enqueue-before-commit",
            Self::ClaimBeforeCommit => "claim-before-commit",
            Self::AckBeforeCommit => "ack-before-commit",
            Self::NackBeforeCommit => "nack-before-commit",
            Self::FailBeforeCommit => "fail-before-commit",
        }
    }
}

struct Configuration {
    failpoint: Failpoint,
    address: String,
}

static CONFIGURATION: OnceLock<Mutex<Option<Configuration>>> = OnceLock::new();

pub fn configure(name: &str, address: &str) -> Result<(), &'static str> {
    let failpoint = Failpoint::parse(name).ok_or("unknown failpoint")?;
    let slot = CONFIGURATION.get_or_init(|| Mutex::new(None));
    let mut configuration = slot.lock().map_err(|_| "failpoint mutex poisoned")?;
    if configuration.is_some() {
        return Err("a failpoint is already configured");
    }
    *configuration = Some(Configuration {
        failpoint,
        address: address.to_owned(),
    });
    Ok(())
}

pub fn hit(failpoint: Failpoint) {
    let Some(slot) = CONFIGURATION.get() else {
        return;
    };
    let Ok(configuration) = slot.lock() else {
        return;
    };
    let Some(configuration) = configuration.as_ref() else {
        return;
    };
    if configuration.failpoint != failpoint {
        return;
    }

    let mut stream = TcpStream::connect(&configuration.address)
        .expect("crash harness listener must be reachable");
    stream
        .write_all(format!("{}\n", failpoint.name()).as_bytes())
        .expect("crash harness notification must be writable");
    stream
        .flush()
        .expect("crash harness notification must flush");
    let mut release = [0u8; 1];
    let _ = stream.read(&mut release);
}
