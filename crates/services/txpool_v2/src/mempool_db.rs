use fuel_core_types::fuel_types::{AssetId, ContractId};
use rusqlite::{params, Connection, Result};
use std::path::Path;

pub struct MempoolDB {
    conn: Connection,
}

impl MempoolDB {
    pub fn new() -> Result<Self> {
        let path = "/tmp/txpool_v2_mempool.db";
        println!("Opening txpool_v2 SQLite database at: {}", path);

        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        let conn = Connection::open(path)?;
        conn.pragma_update(None, "synchronous", "OFF")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;

        // Create table for assets_moved with block number
        conn.execute(
            "CREATE TABLE IF NOT EXISTS assets_moved (
                block INTEGER NOT NULL,
                from_address BLOB NOT NULL,
                asset_id BLOB NOT NULL,
                change INTEGER NOT NULL,
                self_transfer BOOL NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (block, from_address, asset_id)
            )",
            [],
        )?;

        Ok(Self { conn })
    }

    pub fn insert_assets_moved(
        &self,
        block: u32,
        assets_moved: Vec<(ContractId, AssetId, i64, bool)>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "INSERT OR IGNORE INTO assets_moved (block, from_address, asset_id, change, self_transfer) 
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;

        for (from, asset_id, change, self_transfer) in assets_moved {
            stmt.execute(params![
                block,
                from.as_ref(),     // ContractId as bytes
                asset_id.as_ref(), // AssetId as bytes
                change,
                self_transfer
            ])?;
        }
        Ok(())
    }
}
