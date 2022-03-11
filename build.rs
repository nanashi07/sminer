use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["protobuf/yahoo-finance.proto"], &["protobuf/"])?;
    Ok(())
}
