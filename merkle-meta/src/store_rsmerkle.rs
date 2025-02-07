use rs_merkle::{self, algorithms::Sha256, MerkleTree};
use std::error::Error;

use std::{fs, io::Write, path::Path};

const STORAGE_METADATA: &str = "../merkle-meta/metamerkle/";

pub struct RSMerkleTree{
    leaves: Vec<[u8; 32]>,
    rs_tree: MerkleTree<Sha256>,
}

impl RSMerkleTree {
    
    // initiate a Merkle Tree
    pub fn new(leaves: Vec<[u8; 32]>) -> Result<MerkleTree<Sha256>, Box<dyn Error>> {
        let rs_tree = MerkleTree::<Sha256>::from_leaves(&leaves);
        Ok(rs_tree)
    }
}

pub struct PersistentRsMerkle {
    pub storage_path: String,
    pub tree: Option<MerkleTree<Sha256>>
}

impl PersistentRsMerkle {
    
    pub fn new(root_hex: Option<&str>, tree: Option<MerkleTree<Sha256>>) -> Result<Self, Box<dyn Error>>{
        
        let storage_roothex = if let Some(ref tree) = tree {
           
            format!("{}{}/", STORAGE_METADATA, tree.root_hex().ok_or("couldn't get the merkle root")?)
        } else {
            format!("{}{}/", STORAGE_METADATA, root_hex.unwrap_or("default_root_hex"))
        };

        fs::create_dir_all(&storage_roothex)?;

        Ok (Self {
            storage_path: storage_roothex,
            tree,
            })
    }


    // store metadata as individual leaf node
    // pub fn store_leaf(&self, index: usize, metadata: &[u8]) -> Result<(), Box<dyn Error>> {
    //     let leaf_path = format!("{}/leaf_{:04}.bin", self.storage_path, index);
    //     let mut file = fs::File::create(leaf_path)?;
    //     file.write(metadata)?;
    //     Ok(())
    // }

    // store the root
    pub fn store_root(&mut self) -> Result<(), Box<dyn Error>>{
        
        let db_path = format!("{}/root.bin", self.storage_path);
        
        if let Some(ref mut tree) = self.tree{
            tree.commit();
        
            if let Some(root) = tree.root() {
            //let data = bincode::serialize(&root).expect("Failed to serialize bincode root");
                fs::write(db_path, &root)?;//.expect}("Failed to write serialize root");
            }
        }

        Ok(())
    }

    // store the tree structure
    pub fn store_tree(&mut self) -> Result<(), Box<dyn Error>>{
        
        let db_path = format!("{}/tree.bin", self.storage_path);

        if let Some(ref mut tree)= self.tree {
            tree.commit();
            
            let leaves = tree.leaves();

            let flattened_leaves: Vec<u8> = leaves
                .iter()
                .flat_map(|leaf| leaf.iter().copied())
                .flatten()
                .collect();

            fs::write(db_path, &flattened_leaves)?; //.expect("Failed to write serialize tree");
        }

        Ok(())
    }

    pub fn load_tree(&mut self) -> Result<Self, Box<dyn Error>> {

        let db_path = format!("{}/tree.bin", self.storage_path);

        if !Path::new(&db_path).exists() {
            return Err(format!("Tree file not found at {}", db_path).into());
        }

        let data = fs::read(db_path)?;

        // Ensure data is a multiple of 32 bytes (valid `[u8; 32]` leaves)
        if data.len() % 32 != 0 {
            return Err("Invalid tree file size, not a multiple of 32".into());
        }

        let leaves: Vec<[u8; 32]> = data
            .chunks_exact(32)
            .map(|chunk| chunk.try_into().expect("Failed to convert chunk to [u8;32]"))
            .collect();
    
        let loaded_tree = MerkleTree::<Sha256>::from_leaves(&leaves);

        // // verify the tree before providing
        // let is_valid = self.verify_tree()?;
        
        // if ! is_valid {
        //     return  Err("Loaded tree verification failed".into());
        // }

        Ok (Self {
            storage_path: self.storage_path.clone(),
            tree: Some(loaded_tree),
        })
    
    }

    /// Verifies if the stored root matches the reconstructed root
    pub fn verify_tree(&mut self) -> Result<bool, Box<dyn Error>> {
        
        let root_path = format!("{}/root.bin", self.storage_path);

        if let Some(ref mut tree)= self.tree {
            tree.commit();
        
            if Path::new(&root_path).exists() {
                let stored_root = fs::read(root_path)?;

               if stored_root.len() != 32 {
                return Err(format!("Incorrect root size: {:?}", stored_root).into());
            }

            let stored_root: [u8; 32] = stored_root.try_into().map_err(|_| "Failed to convert stored root to [u8; 32]")?;
            let computed_root = tree.root().ok_or("No root found in tree")?;
                return Ok(stored_root == computed_root);
            }
        }
        return Ok(false)
        
    }

    // Del tree, root and leafs of an obsolete record
    pub fn delete_merkle_meta(&mut self) -> Result<(), Box<dyn Error>> {
        
        println!("deleted check{}", self.storage_path);

        if fs::metadata(&self.storage_path).is_ok(){
            fs::remove_dir_all(&self.storage_path)?;
            println!("{} metadata tree-folder deleted successfully", self.storage_path);
        }

        Ok(())
    }

}

// fn main() {

//     let file_cid = "f514drs909f4g9z4w"; // The actual file path
//     let mut metadata_tree = PersistentMetaMerkle::new(file_cid);

//     // Example metadata (3 values per file)
//     let mut metadata_entries = vec![
//         Sha256::hash(b"FROMABOVE"), 
//         Sha256::hash(b"mp4"), 
//         Sha256::hash(b"BXO545")
//     ];

//     // Save each metadata entry
//     for (i, meta) in metadata_entries.iter().enumerate() {
//         metadata_tree.store_leaf(i, meta);
//     }

//     // Add metadata to Merkle tree
//     metadata_tree.tree.append(&mut metadata_entries);

//     metadata_tree.tree.commit();

//     // Save the tree and its root
//     metadata_tree.store_tree();
//     metadata_tree.store_root();

//     println!("Metadata stored in folder '{}'", metadata_tree.storage_path);

    
// }
