pub mod v2 {
    use serde::{Serialize, Deserialize};
    use prost_wkt_types::*;

    include!(concat!(
        env!("OUT_DIR"),
        "/org.apache.flink.statefun.flink.core.polyglot.rs"
    ));

    include!(concat!(
        env!("OUT_DIR"),
        "/org.apache.flink.statefun.flink.io.rs"
    ));
}
