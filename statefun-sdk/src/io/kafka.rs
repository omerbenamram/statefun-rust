//! Provides [KafkaEgress](crate::io::kafka::KafkaEgress) for sending egress messages to Kafka.
//!
//! To use this, import the `KafkaEgress` trait and then use
//! [`kafka_egress()`](crate::io::kafka::KafkaEgress::kafka_egress) or
//! [`kafka_keyed_egress()`](crate::io::kafka::KafkaEgress::kafka_keyed_egress) on an
//! [Effects](crate::Effects) to send messages to Kafka.
//!
//! # Examples
//!
//! ```
//! use prost_wkt_types::Value;
//!
//! use statefun::io::kafka::KafkaEgress;
//! use statefun::{Address, Context, Effects, EgressIdentifier, FunctionRegistry, FunctionType};
//!
//! pub fn relay_to_kafka(_context: Context, message: Value) -> Effects {
//!     let mut effects = Effects::new();
//!
//!     effects.kafka_keyed_egress(
//!         EgressIdentifier::new("example", "greets"),
//!         "greeting",
//!         "the key",
//!         message,
//!     );
//!
//!     effects
//! }
//! ```
use prost::Message;
use statefun_proto::v2::KafkaProducerRecord;

use crate::{Effects, EgressIdentifier};

/// Extension trait for sending egress messages to Kafka using [Effects](crate::Effects).
pub trait KafkaEgress {
    /// Sends the given message to the Kafka topic `topic` via the egress specified using the
    /// `EgressIdentifier`.
    fn kafka_egress<M: Message>(&mut self, identifier: EgressIdentifier, topic: &str, message: M);

    /// Sends the given message to the Kafka topic `topic` via the egress specified using the
    /// `EgressIdentifier`.
    ///
    /// This will set the given key on the message sent to record.
    fn kafka_keyed_egress<M: Message>(
        &mut self,
        identifier: EgressIdentifier,
        topic: &str,
        key: &str,
        message: M,
    );
}

impl KafkaEgress for Effects {
    fn kafka_egress<M: Message>(&mut self, identifier: EgressIdentifier, topic: &str, message: M) {
        let kafka_record = egress_record(None, topic, message);
        self.egress(identifier, kafka_record);
    }

    fn kafka_keyed_egress<M: Message>(
        &mut self,
        identifier: EgressIdentifier,
        topic: &str,
        key: &str,
        message: M,
    ) {
        let kafka_record: KafkaProducerRecord = egress_record(Some(key), topic, message);
        self.egress(identifier, kafka_record);
    }
}

fn egress_record<M: Message>(key: Option<&str>, topic: &str, value: M) -> KafkaProducerRecord {
    KafkaProducerRecord {
        key: key.map(ToOwned::to_owned).unwrap_or_default(),
        value_bytes: value.encode_to_vec(),
        topic: topic.to_owned(),
    }
}
