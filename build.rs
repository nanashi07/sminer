use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(
        &["protobuf/core.proto", "protobuf/yahoo-finance.proto"],
        &["protobuf/"],
    )?;
    Ok(())
}
