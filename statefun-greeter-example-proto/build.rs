fn main() {
    prost_build::compile_protos("src/example.proto", &["src/"]).expect("protoc");
}
