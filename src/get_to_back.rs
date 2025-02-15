// "get" => {

//                 println!("🔍 Raw Input: '{}'", input); // Debugging

//                 // Extract command arguments
//                 let parts: Vec<&str> = input.trim().splitn(6, ' ').collect();
//                 if parts.len() < 6 {
//                     eprintln!("Error: 'get' command requires at least 6 arguments but got {}", parts.len());
//                     continue;
//                 }

//                 let file_name = parts[1];
//                 let file_type = parts[2];
//                 let secret = parts[3];

//                 // parts[4] should now contain the full private_key string, e.g. "120, 160, 253, ... , 250"
//                 let private_key = parts[4];
//                 let nonce = parts[5];

//                 match nodecommand::get_cid_file(
//                     file_name,
//                     file_type,
//                     secret
//                 ).await {
//                     Ok(returned_cid) => {
//                         // let env_decrypt_path = env::var("decrypt_path").expect("decrypt_path not found in .env");
//                         println!("Debug: Returned CID: {:?}", returned_cid);
//                         let decrypt_path = format!("/Volumes/vrona_SSD/rust_premiere/backup_old_mystik>p2p/data/dec/{}{}", file_name, file_type);
//                         nodecommand::get_decrypt_file(private_key, nonce, &returned_cid, &decrypt_path).await?;
//                         println!("File retrieved successfully.")
//                     },
//                     Err(e) => eprintln!("Error retrieving file: {}", e),
//                 }
//             }