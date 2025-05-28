use identity_stronghold::StrongholdStorage;
use iota_sdk::client::secret::stronghold::StrongholdSecretManager;
use iota_sdk::client::Password;
use std::path::PathBuf;
use anyhow::anyhow;


#[tokio::main]
async fn main() -> anyhow::Result<()>{

    // // 1. set snapshot file + password
    // let snapshot_path = PathBuf::from("./wallet.stronghold");
    // let password = Password::from("whatsoever-me-now".to_owned());

    // // 2. build the secret manager (builder pattern)
    // let sm = StrongholdSecretManager::builder()
    //     .password(password)
    //     .build(snapshot_path)?;

    // // 3. wrap it so it implements KwkStorage / KeyIdStorage for 'ssi'
    // let mut store = StrongholdStorage::new(sm);

    // // 4. first-run : create an Ed25519 key labelled "default"
    // if !store.

    Ok(())
}
