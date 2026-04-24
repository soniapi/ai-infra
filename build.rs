fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/ai_infra.proto")?;
    Ok(())
}
