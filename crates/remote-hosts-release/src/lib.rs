//! Native, source-bound release execution. No deployment and no interpreter dependency.
#![forbid(unsafe_code)]
pub mod evidence;
pub mod executor;
pub mod pipeline;
pub mod source;
pub mod test_evidence;
pub mod toolchain;
pub mod updater;
