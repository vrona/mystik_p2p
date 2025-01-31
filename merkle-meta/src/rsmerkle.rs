use rs_merkle::{self, algorithms::Sha256, Hasher, MerkleTree};

use std::{fs, io::Write, path::Path};

const STORAGE_METADATA: &str = "../merkle-meta/metamerkle/";

pub struct PersistentMetaMerkle{
    storage_path: String,
    tree: MerkleTree<Sha256>
}

impl PersistentMetaMerkle {
    

    fn new(cid_storage: &str) -> Self{
        let storage_meta_cid = format!("{}{}/", STORAGE_METADATA, cid_storage);
        fs::create_dir_all(&storage_meta_cid).unwrap();
        
        Self {
            storage_path: storage_meta_cid,
            tree: MerkleTree::<Sha256>::new(),
        }
    }

    // save metadata as individual leaf node
    fn save_leaf(&self, index: usize, metadata: &[u8]) {
        let leaf_path = format!("{}/leaf_{:04}.bin", self.storage_path, index);
        let mut file = fs::File::create(leaf_path).unwrap();
        file.write(metadata).unwrap();
    }

    // save the root
    fn save_root(&mut self) {
        let db_path = format!("{}/root.bin", self.storage_path);
        self.tree.commit();
        let data = bincode::serialize(&self.tree.root().unwrap()).expect("Failed to serialize root");
        fs::write(db_path, data).expect("Failed to write serialize root");
        
    }

    // save the tree structure
    fn save_tree(&mut self) {
        let db_path = format!("{}/tree.bin", self.storage_path);
        self.tree.commit();
        let data = bincode::serialize(&self.tree.leaves().unwrap()).expect("Failed to serialize tree");
        fs::write(db_path, data).expect("Failed to write serialize tree");
    }

    fn save_snapshot() {
        
    }

    fn load_tree(&mut self, cid_storage: &str) {
        let storage_meta_cid = format!("{}{}/", STORAGE_METADATA, cid_storage);
        let db_path = format!("{}/tree.bin", storage_meta_cid);

        if Path::new(&db_path).exists() {
            let data = fs::read(db_path).unwrap();
            let mut leaves: Vec<[u8; 32]> = bincode::deserialize(&data).unwrap();
            self.tree.append(&mut leaves);
        }
    }

    /// Verifies if the stored root matches the reconstructed root
    fn verify_tree(&mut self, cid_storage: &str) -> bool {
        let storage_meta_cid = format!("{}{}/", STORAGE_METADATA, cid_storage);
        let root_path = format!("{}/root.bin", storage_meta_cid);

        self.tree.commit();
        
        if Path::new(&root_path).exists() {
            let stored_root = fs::read(root_path).unwrap();
            let computed_root = self.tree.root().unwrap();
            return stored_root == computed_root;
        }
        return false
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
//         metadata_tree.save_leaf(i, meta);
//     }

//     // Add metadata to Merkle tree
//     metadata_tree.tree.append(&mut metadata_entries);

//     metadata_tree.tree.commit();

//     // Save the tree and its root
//     metadata_tree.save_tree();
//     metadata_tree.save_root();

//     println!("Metadata stored in folder '{}'", metadata_tree.storage_path);

    
// }
