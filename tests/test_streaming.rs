use base64::{engine::general_purpose::STANDARD, Engine};
use clap::builder::Str;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

use hsmq::auth::Claims;
use hsmq::client_factory::ClientFactory;
use hsmq::config::Config;
use hsmq::grpc;
use hsmq::pb::hsmq_client::HsmqClient;
use hsmq::utils;

pub fn get_resource(rel_path: &str) -> PathBuf {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("tests/resources");
    d.push(rel_path);
    d
}

async fn build_server() -> Config {
    let config = Config::from_file(get_resource("hsmq.toml").as_path()).unwrap();
    let sconfig = config.clone();
    let listener = tokio::net::TcpListener::bind(config.node.grpc_address.unwrap())
        .await
        .unwrap();
    tokio::spawn(async move { grpc::run(sconfig, Some(listener)).await.unwrap() });
    config
}

#[tokio::test]
async fn test_streaming() {
    let config = build_server().await;
    let client_factory = ClientFactory::new(config);
    let mut client = client_factory.create_client().await.unwrap();

    let (tx, rx) = mpsc::channel(1);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut stream = client.streaming(stream).await.unwrap().into_inner();

    let topic = "a.b".to_string();
    let mut ids = Vec::with_capacity(5);
    let mut ids_i = Vec::with_capacity(5);

    for i in 0..10 {
        let mut msg = hsmq::pb::Message::default();
        let now = utils::current_time().as_secs_f64().to_string();
        let msg_id = uuid::Uuid::now_v7().to_string();

        msg.topic.clone_from(&topic);
        msg.headers.insert("uuid".to_string(), msg_id.clone());
        msg.headers.insert("ts".to_string(), now);

        let cmd = hsmq::pb::PublishMessage {
            message: Some(msg),
            qos: 1,
            request_id: msg_id.clone(),
        };
        let kind = Some(hsmq::pb::request::Kind::PublishMessage(cmd));
        let req = hsmq::pb::Request { kind };
        tx.send(req).await.unwrap();

        let result = stream.next().await.unwrap().ok();
        assert!(result.is_some_and(|x| true));
        ids.push(msg_id.clone());
        println!("{}", msg_id);
        ids_i.push(i)
    }
    println!("{:?}", ids_i);

    let cmd = hsmq::pb::SubscribeQueue {
        queues: vec!["a".to_string()],
        prefetch_count: 1,
    };
    let kind = Some(hsmq::pb::request::Kind::SubscribeQueue(cmd));
    let req = hsmq::pb::Request { kind };
    tx.send(req).await.unwrap();

    let mut fetched_ids: Vec<String> = Vec::with_capacity(5);
    let mut counter = 0u64;

    for _ in 0..10 {
        let item = stream.next();
        let a = item.await.unwrap();
        match a.clone().unwrap().kind {
            Some(hsmq::pb::response::Kind::Message(msg)) => {
                println!("{:?}", a.clone().unwrap().kind);
                fetched_ids.push(
                    msg.message
                        .clone()
                        .unwrap()
                        .headers
                        .get("uuid")
                        .expect("no message uuid")
                        .to_string(),
                );

                let cmd = hsmq::pb::MessageAck { meta: msg.meta };
                let kind = Some(hsmq::pb::request::Kind::MessageAck(cmd));
                let req = hsmq::pb::Request { kind };
                tx.send(req).await.unwrap();
                let msg = msg.message.unwrap_or_default();
                log::info!("Received: {:?}", utils::repr(&msg));
                counter += 1;
            }
            _ => (),
        }
    }
    assert_eq!(counter, 5);
    assert_eq!(ids[0..5].to_vec(), fetched_ids);
}
