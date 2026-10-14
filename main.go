
package main

import (
	"fmt"
	"sort"
	"sync"
)

// A Row is a flexible set of named columns.
// Different rows can contain different columns.
type Row map[string]any

// Partition holds rows ordered logically by clustering key.
// The map provides efficient point lookups; we sort keys when scanning.
type Partition struct {
	rows map[string]Row
}

// WideColumnDB is a tiny, in-memory, wide-column database.
//
// Logical data model:
//
//	partition key -> clustering key -> column name -> value
type WideColumnDB struct {
	mu         sync.RWMutex
	partitions map[string]*Partition
}

func NewWideColumnDB() *WideColumnDB {
	return &WideColumnDB{
		partitions: make(map[string]*Partition),
	}
}

// Put inserts or updates one cell.
// If the partition, row, or column doesn't exist, it is created.
func (db *WideColumnDB) Put(
	partitionKey string,
	clusteringKey string,
	column string,
	value any,
) {
	db.mu.Lock()
	defer db.mu.Unlock()

	partition, ok := db.partitions[partitionKey]
	if !ok {
		partition = &Partition{
			rows: make(map[string]Row),
		}
		db.partitions[partitionKey] = partition
	}

	row, ok := partition.rows[clusteringKey]
	if !ok {
		row = make(Row)
		partition.rows[clusteringKey] = row
	}

	row[column] = value
}

// PutRow inserts or updates multiple columns in one row.
func (db *WideColumnDB) PutRow(
    partitionKey string,
    clusteringKey string,
    columns Row,
) {
    db.mu.Lock()
    defer db.mu.Unlock()

    partition, ok := db.partitions[partitionKey]
    if !ok {
        partition = &Partition{
            rows: make(map[string]Row),
        }
        db.partitions[partitionKey] = partition
    }

    row, ok := partition.rows[clusteringKey]
    if !ok {
        row = make(Row)
        partition.rows[clusteringKey] = row
    }

    for column, value := range columns {
        row[column] = value
    }
}

// Get reads one cell.
// The boolean is false if the partition, row, or column is missing.
func (db *WideColumnDB) Get(
	partitionKey string,
	clusteringKey string,
	column string,
) (any, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	partition, ok := db.partitions[partitionKey]
	if !ok {
		return nil, false
	}

	row, ok := partition.rows[clusteringKey]
	if !ok {
		return nil, false
	}

	value, ok := row[column]
	return value, ok
}

// GetRow returns a copy of all columns in a row.
func (db *WideColumnDB) GetRow(
	partitionKey string,
	clusteringKey string,
) (Row, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	partition, ok := db.partitions[partitionKey]
	if !ok {
		return nil, false
	}

	row, ok := partition.rows[clusteringKey]
	if !ok {
		return nil, false
	}

	// Return a copy so callers can't mutate internal state.
	result := make(Row, len(row))
	for column, value := range row {
		result[column] = value
	}

	return result, true
}

// ScanPartition returns every row in a partition, sorted by
// clustering key. This is a logical scan, not a disk-efficient scan.
func (db *WideColumnDB) ScanPartition(
	partitionKey string,
) []struct {
	ClusteringKey string
	Columns       Row
} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	partition, ok := db.partitions[partitionKey]
	if !ok {
		return nil
	}

	keys := make([]string, 0, len(partition.rows))
	for key := range partition.rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]struct {
		ClusteringKey string
		Columns       Row
	}, 0, len(keys))

	for _, key := range keys {
		columns := make(Row, len(partition.rows[key]))
		for column, value := range partition.rows[key] {
			columns[column] = value
		}

		result = append(result, struct {
			ClusteringKey string
			Columns       Row
		}{
			ClusteringKey: key,
			Columns:       columns,
		})
	}

	return result
}

func main() {
	db := NewWideColumnDB()

	// Server 1 reports CPU and memory at 10:00.
	db.Put("server-1", "10:00", "cpu", 72)
	db.Put("server-1", "10:00", "memory", 80)

	// At 10:01, it reports CPU and disk instead.
	db.Put("server-1", "10:01", "cpu", 75)
	db.Put("server-1", "10:01", "disk", 45)
	db.Put("server-1", "10:01", "network", 20)

	// A completely different server has different columns.
	db.Put("server-2", "10:00", "cpu", 55)
	db.Put("server-2", "10:00", "temperature", 64)

	// Read a single cell.
	if value, ok := db.Get("server-1", "10:00", "cpu"); ok {
		fmt.Println("CPU at 10:00:", value)
	}

	// Read all columns for one row.
	if row, ok := db.GetRow("server-1", "10:01"); ok {
		fmt.Println("Row at 10:01:", row)
	}

	// Scan all rows for one server.
	fmt.Println("\nAll observations for server-1:")
	for _, row := range db.ScanPartition("server-1") {
		fmt.Printf("%s: %v\n", row.ClusteringKey, row.Columns)
	}
}
