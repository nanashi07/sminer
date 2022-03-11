use sminer::{decoder::deserialize_yahoo_message, Result};

#[tokio::test]
async fn test_deserialize_yahoo_message() -> Result<()> {
    // message
    let content = "CgNTUFkVCpfXQxiQyc6C6F8qA1BDWDAUOABFIBzAv2WAPdLA2AEE";
    let value = deserialize_yahoo_message(content);
    println!("deserialized message: {:?}", value);
    Ok(())
}
