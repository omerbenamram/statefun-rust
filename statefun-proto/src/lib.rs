pub mod v2 {
    include!(concat!(
        env!("OUT_DIR"),
        "/org.apache.flink.statefun.flink.core.polyglot.rs"
    ));

    include!(concat!(
        env!("OUT_DIR"),
        "/org.apache.flink.statefun.flink.io.rs"
    ));
}
