use std::{time::Duration, future::Future};

use rdkafka::{ClientContext, producer::{ProducerContext, ThreadedProducer, FutureProducer, FutureRecord, future_producer::OwnedDeliveryResult}, ClientConfig, Message, error::KafkaError, message::OwnedMessage};

pub struct Producer {
    producer: FutureProducer
}

impl Producer {
    pub fn init() -> Producer{
        Producer {
            producer : ClientConfig::new()
                .set("bootstrap.servers", "localhost:9092")
                .create()
                .expect("Producer creation error")
        }
    }

    pub async fn produce(&self, topic_name: &str, message: &str, key: &str) -> Result<(i32, i64), (KafkaError, OwnedMessage)> { 
        self.producer
            .send(
                FutureRecord::to(topic_name)
                    .payload(&format!("{}", message))
                    .key(&format!("{}", key)),
                    Duration::from_secs(0),
            ).await
    }
}
