package main

import (
	diskstore "github.com/venky1306/LogDB/disk_store"
	LsmTree "github.com/venky1306/LogDB/log_structured_merge_tree"
	wal "github.com/venky1306/LogDB/wal"
)

// DBEngine represents the database engine that manages LSM tree, WAL, and disk store.
type DBEngine struct {
	LsmTree *LsmTree.LSMTree     // LSMTree is the Log-Structured Merge Tree.
	Wal     *wal.WAL             // WAL is the Write-Ahead Log.
	Store   *diskstore.DiskStore // DiskStore manages data storage on disk.
}

// LoadFromDisk loads data from disk into LSM tree and WAL.
func (db *DBEngine) LoadFromDisk(lsmTree *LsmTree.LSMTree, wal *wal.WAL) error {
	return db.Store.LoadFromDisk(lsmTree, wal)
}
