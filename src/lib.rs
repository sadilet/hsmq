pub mod pb {
    tonic::include_proto!("hsmq.v1.hsmq");
}
pub mod auth;
pub mod cluster;
pub mod config;
pub mod errors;
pub mod grpc;
pub mod jwt;
pub mod metrics;
pub mod server;
pub mod tracing;
pub mod utils;
pub mod web;

#[cfg(feature = "consul")]
pub mod consul;

pub mod client_factory;
