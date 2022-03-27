use prost_wkt_build::*;
use std::{
    env,
    ops::Deref,
    path::{Path, PathBuf},
};

fn main() {
    let proto_dir = Path::new(&env::var("CARGO_MANIFEST_DIR").expect("env")).join("src");

    eprintln!("proto_dir={:?}", proto_dir);

    let files = glob::glob(&proto_dir.join("**/*.proto").to_string_lossy())
        .expect("glob")
        .filter_map(|p| p.ok().map(|p| p.to_string_lossy().into_owned()))
        .collect::<Vec<_>>();

    let slices = files.iter().map(Deref::deref).collect::<Vec<_>>();

    eprintln!("protos: {:?}", slices);

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = out.join("descriptors.bin");

    let mut prost_build = prost_build::Config::new();
    prost_build
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .file_descriptor_set_path(&descriptor_file)
        .compile_protos(&slices, &[proto_dir])
        .expect("prost error");

    let descriptor_bytes = std::fs::read(descriptor_file).unwrap();

    let descriptor = FileDescriptorSet::decode(&descriptor_bytes[..]).unwrap();

    prost_wkt_build::add_serde(out, descriptor);
}
