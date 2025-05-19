package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type TableCatalog struct {
	Tables   map[string]*Table
	WAL      *WAL
	inTx     bool
	txBuffer []string
	mu       sync.Mutex
}

func NewTableCatalog() *TableCatalog {
	wal, err := NewWAL("wal.log")
	if err != nil {
		panic(err)
	}
	catalog := &TableCatalog{
		Tables:   make(map[string]*Table),
		WAL:      wal,
		inTx:     false,
		txBuffer: nil,
	}
	// WAL recovery: replay all log entries
	entries, err := wal.Replay()
	if err != nil {
		panic(err)
	}
	for _, entry := range entries {
		_ = executeWriteSQL(catalog, string(entry)) // Ignore errors for recovery
	}
	return catalog
}

func (tc *TableCatalog) CreateTable(name string, schema []string) {
	tc.Tables[name] = &Table{
		Name:    name,
		Schema:  schema,
		BTree:   NewTable(name, schema).BTree,
		Indexes: NewIndexCatalog(),
	}
}

func (tc *TableCatalog) CreateIndex(tableName, indexName string, columns []string) error {
	tbl, ok := tc.Tables[tableName]
	if !ok {
		return fmt.Errorf("table not found: %s", tableName)
	}
	return tbl.AddIndex(indexName, columns)
}

func (tc *TableCatalog) GetTable(name string) (*Table, bool) {
	tbl, ok := tc.Tables[name]
	return tbl, ok
}

func ExecuteSQL(catalog *TableCatalog, sql string) error {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return err
	}

	switch s := stmt.(type) {
	case *CreateTableStatement:
		catalog.CreateTable(s.Table, s.Schema)
		fmt.Printf("Table '%s' created.\n", s.Table)
		return nil
	case *CreateIndexStatement:
		if err := catalog.CreateIndex(s.Table, s.Index, s.Columns); err != nil {
			return err
		}
		fmt.Printf("Index '%s' created on table '%s'.\n", s.Index, s.Table)
		return nil
	}

	// Concurrency control: lock for all write/transaction operations
	isWrite := false
	switch stmt.(type) {
	case *InsertStatement, *UpdateStatement, *DeleteStatement, *BeginStatement, *CommitStatement, *RollbackStatement:
		isWrite = true
	}
	if isWrite {
		catalog.mu.Lock()
		defer catalog.mu.Unlock()
	}

	switch stmt.(type) {
	case *BeginStatement:
		if catalog.inTx {
			return fmt.Errorf("transaction already in progress")
		}
		catalog.inTx = true
		catalog.txBuffer = []string{}
		fmt.Println("  Transaction started.")
		return nil
	case *CommitStatement:
		if !catalog.inTx {
			return fmt.Errorf("no transaction in progress")
		}
		// Write all buffered statements to WAL and apply
		for _, txSQL := range catalog.txBuffer {
			if err := catalog.WAL.Append([]byte(txSQL)); err != nil {
				return fmt.Errorf("WAL append failed: %w", err)
			}
			if err := executeWriteSQL(catalog, txSQL); err != nil {
				return fmt.Errorf("commit failed: %w", err)
			}
		}
		if err := catalog.WAL.Sync(); err != nil {
			return fmt.Errorf("WAL sync failed: %w", err)
		}
		catalog.inTx = false
		catalog.txBuffer = nil
		fmt.Println("  Transaction committed.")
		return nil
	case *RollbackStatement:
		if !catalog.inTx {
			return fmt.Errorf("no transaction in progress")
		}
		catalog.inTx = false
		catalog.txBuffer = nil
		fmt.Println("  Transaction rolled back.")
		return nil
	case *InsertStatement, *UpdateStatement, *DeleteStatement:
		if catalog.inTx {
			catalog.txBuffer = append(catalog.txBuffer, sql)
			fmt.Println("  Queued in transaction.")
			return nil
		}
		// Not in transaction: log to WAL, then apply
		if err := catalog.WAL.Append([]byte(sql)); err != nil {
			return fmt.Errorf("WAL append failed: %w", err)
		}
		if err := catalog.WAL.Sync(); err != nil {
			return fmt.Errorf("WAL sync failed: %w", err)
		}
		return executeWriteSQL(catalog, sql)
	default:
		// SELECT and others: just execute
		return executeWriteSQL(catalog, sql)
	}
}

func executeWriteSQL(catalog *TableCatalog, sql string) error {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return err
	}

	switch s := stmt.(type) {
	case *InsertStatement:
		tbl, ok := catalog.Tables[s.Table]
		if !ok {
			return fmt.Errorf("table not found: %s", s.Table)
		}
		// s.Values is already map[string]string, which Table.Insert expects
		return tbl.Insert(s.Values)
	case *SelectStatement:
		tbl, ok := catalog.Tables[s.Table]
		if !ok {
			return fmt.Errorf("table not found: %s", s.Table)
		}
		resultsMap := []map[string]string{}
		for _, recMap := range tbl.Scan() {
			match := true
			if s.Where != nil {
				for k, v := range s.Where {
					if val, ok := recMap[k]; ok {
						if k == "id" || k == "age" {
							valNum, errVal := strconv.Atoi(val)
							vNum, errV := strconv.Atoi(v)
							if errVal != nil || errV != nil || valNum != vNum {
								match = false
								break
							}
						} else {
							if val != v {
								match = false
								break
							}
						}
					} else {
						match = false
						break
					}
				}
			}
			if match {
				resultsMap = append(resultsMap, recMap)
			}
		}
		// ORDER BY (support id, name, age)
		if s.OrderBy != "" {
			col := strings.ToLower(s.OrderBy)
			sort.Slice(resultsMap, func(i, j int) bool {
				valI, okI := resultsMap[i][col]
				valJ, okJ := resultsMap[j][col]
				if !okI || !okJ { // Handle cases where the order by column might be missing
					return false // Or some other default behavior
				}
				if col == "id" || col == "age" { // Numeric sort
					numI, _ := strconv.Atoi(valI)
					numJ, _ := strconv.Atoi(valJ)
					return numI < numJ
				}
				return valI < valJ // String sort
			})
		}
		// LIMIT
		if s.Limit > 0 && len(resultsMap) > s.Limit {
			resultsMap = resultsMap[:s.Limit]
		}
		// Print results
		if len(resultsMap) == 0 {
			fmt.Println("  (no rows)")
		} else {
			// Print header and rows using prettyPrintTable
			var header []string
			if len(s.Columns) == 1 && s.Columns[0] == "*" {
				header = tbl.Schema
			} else {
				header = s.Columns
			}
			rows := [][]string{}
			for _, r := range resultsMap {
				row := make([]string, len(header))
				for i, h := range header {
					row[i] = r[h]
				}
				rows = append(rows, row)
			}
			prettyPrintTable(header, rows)
		}
		return nil
	case *UpdateStatement:
		tbl, ok := catalog.Tables[s.Table]
		if !ok {
			return fmt.Errorf("table not found: %s", s.Table)
		}
		count := 0
		allRecords := tbl.Scan()
		updatedPks := []string{}

		for _, recMap := range allRecords {
			pkVal, pkOk := recMap["id"]
			if !pkOk {
				continue
			}

			alreadyUpdated := false
			for _, updatedPk := range updatedPks {
				if pkVal == updatedPk {
					alreadyUpdated = true
					break
				}
			}
			if alreadyUpdated {
				continue
			}

			match := true
			if s.Where != nil { // Ensure Where is not nil
				for k, v := range s.Where {
					if val, ok := recMap[k]; ok {
						if k == "id" || k == "age" {
							valNum, errVal := strconv.Atoi(val)
							vNum, errV := strconv.Atoi(v)
							if errVal != nil || errV != nil || valNum != vNum {
								match = false
								break
							}
						} else {
							if val != v {
								match = false
								break
							}
						}
					} else {
						match = false
						break
					}
				}
			}
			if match {
				updatedRecMap := make(map[string]string)
				for k, v := range recMap {
					updatedRecMap[k] = v
				}
				for k, v := range s.Set {
					updatedRecMap[k] = v
				}
				err := tbl.Insert(updatedRecMap) // This will update if ID exists, or insert if ID changes to new
				if err != nil {
					return fmt.Errorf("error updating record %s: %w", pkVal, err)
				}
				updatedPks = append(updatedPks, updatedRecMap["id"]) // Use ID from updatedRecMap in case ID itself was changed
				count++
			}
		}
		fmt.Printf("  %d row(s) updated\n", count)
		return nil
	case *DeleteStatement:
		tbl, ok := catalog.Tables[s.Table]
		if !ok {
			return fmt.Errorf("table not found: %s", s.Table)
		}
		count := 0
		idsToDelete := []string{}
		allRecords := tbl.Scan()

		for _, recMap := range allRecords {
			match := true
			if s.Where != nil { // Ensure Where is not nil
				for k, v := range s.Where {
					if val, ok := recMap[k]; ok {
						if k == "id" || k == "age" {
							valNum, errVal := strconv.Atoi(val)
							vNum, errV := strconv.Atoi(v)
							if errVal != nil || errV != nil || valNum != vNum {
								match = false
								break
							}
						} else {
							if val != v {
								match = false
								break
							}
						}
					} else {
						match = false
						break
					}
				}
			}
			if match {
				if idVal, idOk := recMap["id"]; idOk {
					idsToDelete = append(idsToDelete, idVal)
				}
			}
		}
		for _, idStr := range idsToDelete {
			tbl.BTree.Delete([]byte(idStr))
			count++
		}
		fmt.Printf("  %d row(s) deleted\n", count)
		return nil
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
	BNODE_NODE         = 1 // internal node
	BNODE_LEAF         = 2 // leaf node
)

type BNode []byte

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// Helper: is leaf node
func (node BNode) isLeaf() bool {
	return node.btype() == BNODE_LEAF
}

// For leaf nodes, PTRS are not used, so offsets start at HEADER
func offsetPos(node BNode, idx uint16) uint16 {
	if node.isLeaf() {
		return HEADER + 2*(idx-1)
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// For leaf nodes, key/vals start after HEADER + 2*nkeys
func (node BNode) kvPos(idx uint16) uint16 {
	if node.isLeaf() {
		return HEADER + 2*node.nkeys() + node.getOffset(idx)
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node BNode) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4 : pos+4+klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen : pos+4+klen+vlen]
}

// BTree struct with in-memory page management

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

// In-memory page store for testing

type memPages struct {
	pages map[uint64]BNode
	next  uint64
}

func newMemPages() *memPages {
	return &memPages{
		pages: make(map[uint64]BNode),
		next:  1,
	}
}

func (m *memPages) get(ptr uint64) []byte {
	page := m.pages[ptr]
	// fmt.Printf("[DEBUG] memPages.get: ptr=%d, found=%v\n", ptr, page != nil)
	return page
}

func (m *memPages) new(node []byte) uint64 {
	ptr := m.next
	m.next++
	copyNode := make([]byte, len(node))
	copy(copyNode, node)
	m.pages[ptr] = copyNode
	// fmt.Printf("[DEBUG] memPages.new: ptr=%d, len=%d\n", ptr, len(copyNode))
	return ptr
}

func (m *memPages) del(ptr uint64) {
	delete(m.pages, ptr)
}

// Helper: compare two byte slices
func cmp(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Search for a key in the B+Tree
func (t *BTree) Get(key []byte) ([]byte, bool) {
	// fmt.Printf("[DEBUG] BTree.Get: key=%v, root=%d\n", key, t.root)
	if t.root == 0 {
		return nil, false
	}
	ptr := t.root
	for {
		node := BNode(t.get(ptr))
		n := node.nkeys()
		if node.isLeaf() {
			for i := uint16(1); i <= n; i++ {
				k := node.getKey(i)
				cmpRes := cmp(key, k)
				// fmt.Printf("[DEBUG] BTree.Get: compare key=%v with node.getKey(%d)=%v, cmpRes=%d\n", key, i, k, cmpRes)
				if cmpRes == 0 {
					return node.getVal(i), true
				}
			}
			return nil, false
		}
		// Internal node: find child
		var idx uint16 = 1
		for ; idx <= n; idx++ {
			if cmp(key, node.getKey(idx)) < 0 {
				break
			}
		}
		ptr = node.getPtr(idx - 1)
	}
}

// Insert a key/value into a single-leaf B+Tree (no split)
func (t *BTree) Set(key, val []byte) bool {
	// fmt.Printf("[DEBUG] BTree.Set: key=%v, valLen=%d, root=%d\n", key, len(val), t.root)
	if t.root == 0 {
		leaf := make(BNode, BTREE_PAGE_SIZE)
		leaf.setHeader(BNODE_LEAF, 1)
		leaf.setOffset(1, 0)
		kvPos := leaf.kvPos(1)
		binary.LittleEndian.PutUint16(leaf[kvPos:], uint16(len(key)))
		binary.LittleEndian.PutUint16(leaf[kvPos+2:], uint16(len(val)))
		copy(leaf[kvPos+4:], key)
		copy(leaf[kvPos+4+uint16(len(key)):], val)
		t.root = t.new(leaf)
		// fmt.Printf("[DEBUG] BTree.Set: new root=%d\n", t.root)
		return true
	}
	newRoot, _ := t.insert(t.root, key, val)
	if newRoot != 0 {
		t.root = newRoot
		// fmt.Printf("[DEBUG] BTree.Set: updated root=%d\n", t.root)
	}
	return true
}

// Recursive insert, returns new node ptr if split, else 0
func (t *BTree) insert(ptr uint64, key, val []byte) (uint64, []byte) {
	node := BNode(t.get(ptr))
	if node.isLeaf() {
		return t.insertLeaf(ptr, key, val)
	}
	// Internal node: find child
	n := node.nkeys()
	var idx uint16 = 1
	for ; idx <= n; idx++ {
		if cmp(key, node.getKey(idx)) < 0 {
			break
		}
	}
	childPtr := node.getPtr(idx - 1)
	newChildPtr, splitKey := t.insert(childPtr, key, val)
	if newChildPtr == 0 {
		return 0, nil
	}
	// Insert splitKey and newChildPtr into this node
	return t.insertInternal(ptr, splitKey, newChildPtr)
}

// Insert into leaf, split if full
func (t *BTree) insertLeaf(ptr uint64, key, val []byte) (uint64, []byte) {
	node := BNode(t.get(ptr))
	n := node.nkeys()
	// Find insert/update position
	var idx uint16 = 1
	for ; idx <= n; idx++ {
		cmpRes := cmp(key, node.getKey(idx))
		if cmpRes == 0 {
			// Update value
			// For simplicity, do not shrink/grow, just overwrite if same size
			v := node.getVal(idx)
			if len(v) == len(val) {
				copy(v, val)
				return 0, nil
			}
			break // Remove and re-insert
		}
		if cmpRes < 0 {
			break
		}
	}
	// Build new node with key inserted at idx
	tmp := make(BNode, BTREE_PAGE_SIZE)
	tmp.setHeader(BNODE_LEAF, n+1)
	// Copy offsets and key/vals
	var off uint16 = 0
	for i, j := uint16(1), uint16(1); i <= n+1; i++ {
		if i == idx {
			tmp.setOffset(i, off)
			kvPos := tmp.kvPos(i)
			binary.LittleEndian.PutUint16(tmp[kvPos:], uint16(len(key)))
			binary.LittleEndian.PutUint16(tmp[kvPos+2:], uint16(len(val)))
			copy(tmp[kvPos+4:], key)
			copy(tmp[kvPos+4+uint16(len(key)):], val)
			off += 4 + uint16(len(key)) + uint16(len(val))
		} else {
			k := node.getKey(j)
			v := node.getVal(j)
			if j == idx && cmp(key, k) == 0 {
				j++ // skip old key if updating
				k = node.getKey(j)
				v = node.getVal(j)
			}
			tmp.setOffset(i, off)
			kvPos := tmp.kvPos(i)
			binary.LittleEndian.PutUint16(tmp[kvPos:], uint16(len(k)))
			binary.LittleEndian.PutUint16(tmp[kvPos+2:], uint16(len(v)))
			copy(tmp[kvPos+4:], k)
			copy(tmp[kvPos+4+uint16(len(k)):], v)
			off += 4 + uint16(len(k)) + uint16(len(v))
			j++
		}
	}
	// If fits, overwrite
	if int(off)+int(HEADER)+2*int(n+1) <= BTREE_PAGE_SIZE {
		copy(node, tmp)
		return 0, nil
	}
	// Split
	mid := (n + 1) / 2
	left := make(BNode, BTREE_PAGE_SIZE)
	left.setHeader(BNODE_LEAF, mid)
	var offL uint16 = 0
	for i := uint16(1); i <= mid; i++ {
		left.setOffset(i, offL)
		k := tmp.getKey(i)
		v := tmp.getVal(i)
		kvPos := left.kvPos(i)
		binary.LittleEndian.PutUint16(left[kvPos:], uint16(len(k)))
		binary.LittleEndian.PutUint16(left[kvPos+2:], uint16(len(v)))
		copy(left[kvPos+4:], k)
		copy(left[kvPos+4+uint16(len(k)):], v)
		offL += 4 + uint16(len(k)) + uint16(len(v))
	}
	right := make(BNode, BTREE_PAGE_SIZE)
	right.setHeader(BNODE_LEAF, n+1-mid)
	var offR uint16 = 0
	for i := mid + 1; i <= n+1; i++ {
		right.setOffset(i-mid, offR)
		k := tmp.getKey(i)
		v := tmp.getVal(i)
		kvPos := right.kvPos(i - mid)
		binary.LittleEndian.PutUint16(right[kvPos:], uint16(len(k)))
		binary.LittleEndian.PutUint16(right[kvPos+2:], uint16(len(v)))
		copy(right[kvPos+4:], k)
		copy(right[kvPos+4+uint16(len(k)):], v)
		offR += 4 + uint16(len(k)) + uint16(len(v))
	}
	// Link leaves (not implemented, but could add next ptr)
	// Promote first key of right
	promote := right.getKey(1)
	leftPtr := ptr
	copy(node, left)
	rightPtr := t.new(right)
	return t.newInternal(promote, leftPtr, rightPtr), promote
}

// Insert into internal node, split if full
func (t *BTree) insertInternal(ptr uint64, key []byte, newChildPtr uint64) (uint64, []byte) {
	node := BNode(t.get(ptr))
	n := node.nkeys()
	// Find insert position
	var idx uint16 = 1
	for ; idx <= n; idx++ {
		if cmp(key, node.getKey(idx)) < 0 {
			break
		}
	}
	tmp := make(BNode, BTREE_PAGE_SIZE)
	tmp.setHeader(BNODE_NODE, n+1)
	// Copy ptrs, keys
	for i, j := uint16(0), uint16(0); i <= n; i++ {
		if i == idx-1 {
			tmp.setPtr(i, newChildPtr)
		} else {
			tmp.setPtr(i, node.getPtr(j))
			j++
		}
	}
	var off uint16 = 0
	for i, j := uint16(1), uint16(1); i <= n+1; i++ {
		if i == idx {
			tmp.setOffset(i, off)
			kvPos := tmp.kvPos(i)
			binary.LittleEndian.PutUint16(tmp[kvPos:], uint16(len(key)))
			binary.LittleEndian.PutUint16(tmp[kvPos+2:], 0)
			copy(tmp[kvPos+4:], key)
			off += 4 + uint16(len(key))
		} else {
			k := node.getKey(j)
			tmp.setOffset(i, off)
			kvPos := tmp.kvPos(i)
			binary.LittleEndian.PutUint16(tmp[kvPos:], uint16(len(k)))
			binary.LittleEndian.PutUint16(tmp[kvPos+2:], 0)
			copy(tmp[kvPos+4:], k)
			off += 4 + uint16(len(k))
			j++
		}
	}
	if int(off)+int(HEADER)+8*int(n+2)+2*int(n+1) <= BTREE_PAGE_SIZE {
		copy(node, tmp)
		return 0, nil
	}
	// Split internal node
	mid := (n + 1) / 2
	left := make(BNode, BTREE_PAGE_SIZE)
	left.setHeader(BNODE_NODE, mid)
	for i := uint16(0); i < mid; i++ {
		left.setPtr(i, tmp.getPtr(i))
	}
	var offL uint16 = 0
	for i := uint16(1); i <= mid; i++ {
		left.setOffset(i, offL)
		k := tmp.getKey(i)
		kvPos := left.kvPos(i)
		binary.LittleEndian.PutUint16(left[kvPos:], uint16(len(k)))
		binary.LittleEndian.PutUint16(left[kvPos+2:], 0)
		copy(left[kvPos+4:], k)
		offL += 4 + uint16(len(k))
	}
	right := make(BNode, BTREE_PAGE_SIZE)
	right.setHeader(BNODE_NODE, n+1-mid)
	for i := uint16(mid); i <= n+1; i++ {
		right.setPtr(i-mid, tmp.getPtr(i))
	}
	var offR uint16 = 0
	for i := mid + 1; i <= n+1; i++ {
		right.setOffset(i-mid, offR)
		k := tmp.getKey(i)
		kvPos := right.kvPos(i - mid)
		binary.LittleEndian.PutUint16(right[kvPos:], uint16(len(k)))
		binary.LittleEndian.PutUint16(right[kvPos+2:], 0)
		copy(right[kvPos+4:], k)
		offR += 4 + uint16(len(k))
	}
	promote := tmp.getKey(mid + 1)
	leftPtr := ptr
	copy(node, left)
	rightPtr := t.new(right)
	return t.newInternal(promote, leftPtr, rightPtr), promote
}

// Create new root/internal node
func (t *BTree) newInternal(key []byte, leftPtr, rightPtr uint64) uint64 {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_NODE, 1)
	node.setPtr(0, leftPtr)
	node.setPtr(1, rightPtr)
	node.setOffset(1, 0)
	kvPos := node.kvPos(1)
	binary.LittleEndian.PutUint16(node[kvPos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[kvPos+2:], 0)
	copy(node[kvPos+4:], key)
	return t.new(node)
}

// Delete a key from the B+Tree
func (t *BTree) Delete(key []byte) bool {
	if t.root == 0 {
		return false
	}
	newRoot, changed := t.delete(t.root, key)
	if changed {
		if newRoot != 0 {
			t.root = newRoot
		} else if BNode(t.get(t.root)).nkeys() == 0 {
			t.root = 0 // tree is empty
		}
	}
	return changed
}

// Recursive delete, returns new root ptr if root shrinks, and whether tree changed
func (t *BTree) delete(ptr uint64, key []byte) (uint64, bool) {
	node := BNode(t.get(ptr))
	if node.isLeaf() {
		return t.deleteLeaf(ptr, key)
	}
	n := node.nkeys()
	var idx uint16 = 1
	for ; idx <= n; idx++ {
		if cmp(key, node.getKey(idx)) < 0 {
			break
		}
	}
	childPtr := node.getPtr(idx - 1)
	newChildPtr, changed := t.delete(childPtr, key)
	if !changed {
		return 0, false
	}
	if newChildPtr != 0 {
		node.setPtr(idx-1, newChildPtr)
	}
	// If child underflowed and merged, remove key/ptr from this node
	childNode := BNode(t.get(node.getPtr(idx - 1)))
	if childNode.nkeys() == 0 {
		// Remove key/ptr at idx-1
		tmp := make(BNode, BTREE_PAGE_SIZE)
		tmp.setHeader(BNODE_NODE, n-1)
		for i, j := uint16(0), uint16(0); i <= n; i++ {
			if i == idx-1 {
				continue
			}
			tmp.setPtr(j, node.getPtr(i))
			j++
		}
		var off uint16 = 0
		for i, j := uint16(1), uint16(1); i <= n; i++ {
			if i == idx {
				continue
			}
			tmp.setOffset(j, off)
			k := node.getKey(i)
			kvPos := tmp.kvPos(j)
			binary.LittleEndian.PutUint16(tmp[kvPos:], uint16(len(k)))
			binary.LittleEndian.PutUint16(tmp[kvPos+2:], 0)
			copy(tmp[kvPos+4:], k)
			off += 4 + uint16(len(k))
			j++
		}
		if tmp.nkeys() == 0 {
			return node.getPtr(0), true // root shrinks
		}
		copy(node, tmp)
		return 0, true
	}
	return 0, true
}

// Delete from leaf node
func (t *BTree) deleteLeaf(ptr uint64, key []byte) (uint64, bool) {
	node := BNode(t.get(ptr))
	n := node.nkeys()
	var idx uint16 = 0
	for i := uint16(1); i <= n; i++ {
		if cmp(key, node.getKey(i)) == 0 {
			idx = i
			break
		}
	}
	if idx == 0 {
		return 0, false // not found
	}
	// Build new node without the key
	tmp := make(BNode, BTREE_PAGE_SIZE)
	tmp.setHeader(BNODE_LEAF, n-1)
	var off uint16 = 0
	for i, j := uint16(1), uint16(1); i <= n; i++ {
		if i == idx {
			continue
		}
		tmp.setOffset(j, off)
		k := node.getKey(i)
		v := node.getVal(i)
		kvPos := tmp.kvPos(j)
		binary.LittleEndian.PutUint16(tmp[kvPos:], uint16(len(k)))
		binary.LittleEndian.PutUint16(tmp[kvPos+2:], uint16(len(v)))
		copy(tmp[kvPos+4:], k)
		copy(tmp[kvPos+4+uint16(len(k)):], v)
		off += 4 + uint16(len(k)) + uint16(len(v))
		j++
	}
	copy(node, tmp)
	return 0, true
}

// For debugging: print the entire BTree structure
func (t *BTree) PrintTree() {
	fmt.Println("--- BTree Structure ---")
	if t.root == 0 {
		fmt.Println("(empty)")
		return
	}
	t.printNode(t.root, 0)
	fmt.Println("-----------------------")
}

func (t *BTree) printNode(ptr uint64, level int) {
	node := BNode(t.get(ptr))
	indent := strings.Repeat("  ", level)
	fmt.Printf("%sNode %d (%s, %d keys):\n", indent, ptr, nodeTypeStr(node.btype()), node.nkeys())

	if node.isLeaf() {
		for i := uint16(1); i <= node.nkeys(); i++ {
			fmt.Printf("%s  - Key: %s, Val: %s\n", indent, string(node.getKey(i)), string(node.getVal(i)))
		}
	} else {
		for i := uint16(0); i <= node.nkeys(); i++ {
			if i > 0 {
				fmt.Printf("%s  - Key %d: %s\n", indent, i, string(node.getKey(i)))
			}
			t.printNode(node.getPtr(i), level+1)
		}
	}
}

func nodeTypeStr(btype uint16) string {
	if btype == BNODE_LEAF {
		return "LEAF"
	}
	return "INTERNAL"
}

type Table struct {
	Name    string
	Schema  []string
	BTree   *BTree
	Indexes *IndexCatalog
}

func NewTable(name string, schema []string) *Table {
	mp := newMemPages()
	return &Table{
		Name:   name,
		Schema: schema,
		BTree: &BTree{
			get: mp.get,
			new: mp.new,
			del: mp.del,
		},
		Indexes: NewIndexCatalog(),
	}
}

type Index struct {
	Name    string
	Columns []string
	BTree   *BTree
}

type IndexCatalog struct {
	Indexes map[string]*Index
}

func NewIndexCatalog() *IndexCatalog {
	return &IndexCatalog{Indexes: make(map[string]*Index)}
}

// AddIndex creates a new secondary index on the table
func (tbl *Table) AddIndex(name string, columns []string) error {
	idx := &Index{
		Name:    name,
		Columns: columns,
		BTree:   &BTree{get: tbl.BTree.get, new: tbl.BTree.new, del: tbl.BTree.del}, // Use same pager
	}
	tbl.Indexes.Indexes[name] = idx
	return nil
}

// Update Insert to maintain secondary indexes
func (tbl *Table) Insert(record map[string]string) error {
	if record["version"] == "" {
		record["version"] = "1"
	} else {
		v, _ := strconv.Atoi(record["version"])
		record["version"] = strconv.Itoa(v + 1)
	}
	row, err := MapToRow(record, tbl.Schema)
	if err != nil {
		return err
	}
	// Use the first column in the schema as the primary key
	pkCol := tbl.Schema[0]
	pkVal, ok := record[pkCol]
	if !ok {
		return fmt.Errorf("missing primary key column '%s' for key", pkCol)
	}
	tbl.BTree.Set([]byte(pkVal), row)
	// Maintain secondary indexes
	for _, idx := range tbl.Indexes.Indexes {
		key := ""
		for _, col := range idx.Columns {
			key += record[col] + "|"
		}
		idx.BTree.Set([]byte(key), []byte(pkVal))
	}
	return nil
}

func (tbl *Table) Get(id string) (map[string]string, bool) {
	// fmt.Printf("[DEBUG] Table.Get: id=%s\n", id)
	row, found := tbl.BTree.Get([]byte(id))
	if !found {
		return nil, false
	}
	// fmt.Printf("[DEBUG] Table.Get: found rowData=%v\n", row)
	recMap, err := RowToMap(row, tbl.Schema)
	if err != nil {
		return nil, false // Error during conversion, treat as not found or handle error appropriately
	}
	return recMap, true
}

// Scan all records in the table
func (tbl *Table) Scan() []map[string]string {
	// fmt.Printf("[DEBUG] Table.Scan: starting scan\n")
	var records []map[string]string
	if tbl.BTree.root == 0 {
		// fmt.Printf("[DEBUG] Table.Scan: empty tree\n")
		return records
	}
	tbl.scanNode(tbl.BTree.root, &records)
	// fmt.Printf("[DEBUG] Table.Scan: found %d records\n", len(records))
	return records
}

func (tbl *Table) scanNode(ptr uint64, records *[]map[string]string) {
	node := BNode(tbl.BTree.get(ptr))
	// fmt.Printf("[DEBUG] Table.scanNode: ptr=%d, type=%s, nkeys=%d\n", ptr, nodeTypeStr(node.btype()), node.nkeys())
	if node.isLeaf() {
		for i := uint16(1); i <= node.nkeys(); i++ {
			val := node.getVal(i)
			// key := node.getKey(i) // For debugging
			// fmt.Printf("[DEBUG] Table.scanNode Leaf: key=%s, valLen=%d\n", string(key), len(val))
			rec, err := RowToMap(val, tbl.Schema)
			if err == nil {
				*records = append(*records, rec)
			} else {
				// fmt.Printf("[ERROR] Table.scanNode: error converting row: %v\n", err)
			}
		}
	} else {
		for i := uint16(0); i <= node.nkeys(); i++ {
			tbl.scanNode(node.getPtr(i), records)
		}
	}
}

// Pretty print a table
func prettyPrintTable(header []string, rows [][]string) {
	if len(header) == 0 {
		return
	}
	colWidths := make([]int, len(header))
	for i, h := range header {
		colWidths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}
	// Print header
	fmt.Print("| ")
	for i, h := range header {
		fmt.Printf("%-*s | ", colWidths[i], h)
	}
	fmt.Println()
	// Print separator
	fmt.Print("|-")
	for _, w := range colWidths {
		fmt.Print(strings.Repeat("-", w), "-+-")
	}
	fmt.Println()
	// Print rows
	for _, row := range rows {
		fmt.Print("| ")
		for i, cell := range row {
			fmt.Printf("%-*s | ", colWidths[i], cell)
		}
		fmt.Println()
	}
}

type Database struct {
	Tables map[string]*Table
}

func NewDatabase() *Database {
	return &Database{
		Tables: make(map[string]*Table),
	}
}

// This CreateTable is part of the old Database struct.
// The new one is part of TableCatalog.
func (db *Database) CreateTable(name string, schema []string) {
	// This is now handled by catalog.CreateTable
	// For compatibility, if someone were to use the old NewDatabase(),
	// this would still work but not use the new BTree injection.
	// Ideally, this old Database struct and its methods should be removed.
	db.Tables[name] = NewTable(name, schema) // NewTable still exists but doesn't take BTree
	fmt.Printf("Table '%s' created (using old Database.CreateTable).\n", name)
}

// This ExecuteSQL is part of the old Database struct.
// The new one is a standalone function.
func (db *Database) ExecuteSQL(sql string) error {
	fmt.Println("[INFO] Using old Database.ExecuteSQL method. Consider migrating to standalone ExecuteSQL with TableCatalog.")
	// This is a simplified version or could delegate to the new one if a catalog were available.
	// For now, it's a placeholder to show it's the old one.
	return fmt.Errorf("old Database.ExecuteSQL is deprecated; use standalone ExecuteSQL function with TableCatalog")
}

type MetaPage struct {
	TableRoots map[string]uint64
}

func LoadMetaPage(pager *Pager) (*MetaPage, error) {
	buf, err := pager.Get(0)
	if err != nil {
		return nil, err
	}
	meta := &MetaPage{TableRoots: make(map[string]uint64)}
	n := int(buf[0])
	offset := 1
	for i := 0; i < n; i++ {
		nameLen := int(buf[offset])
		offset++
		name := string(buf[offset : offset+nameLen])
		offset += nameLen
		root := binary.LittleEndian.Uint64(buf[offset : offset+8])
		offset += 8
		meta.TableRoots[name] = root
	}
	return meta, nil
}

func SaveMetaPage(pager *Pager, meta *MetaPage) error {
	buf, err := pager.Get(0)
	if err != nil {
		return err
	}
	offset := 1
	buf[0] = byte(len(meta.TableRoots))
	for name, root := range meta.TableRoots {
		buf[offset] = byte(len(name))
		offset++
		copy(buf[offset:], []byte(name))
		offset += len(name)
		binary.LittleEndian.PutUint64(buf[offset:], root)
		offset += 8
	}
	return pager.Flush(0)
}

type DiskTableCatalog struct {
	Tables map[string]*Table
	Pager  *Pager
	Meta   *MetaPage
}

func NewDiskTableCatalog(pager *Pager) *DiskTableCatalog {
	meta, err := LoadMetaPage(pager)
	if err != nil {
		meta = &MetaPage{TableRoots: make(map[string]uint64)}
	}
	tables := make(map[string]*Table)
	for name, root := range meta.TableRoots {
		tables[name] = &Table{
			Name:   name,
			Schema: []string{"id", "name", "age"}, // TODO: load schema from meta page if needed
			BTree:  NewDiskBTree(pager, root),
		}
	}
	return &DiskTableCatalog{
		Tables: tables,
		Pager:  pager,
		Meta:   meta,
	}
}

func (dc *DiskTableCatalog) CreateTable(name string, schema []string) error {
	// Allocate a new root page for the table
	root, _, err := dc.Pager.New()
	if err != nil {
		return err
	}
	dc.Tables[name] = &Table{
		Name:   name,
		Schema: schema,
		BTree:  NewDiskBTree(dc.Pager, root),
	}
	dc.Meta.TableRoots[name] = root
	return SaveMetaPage(dc.Pager, dc.Meta)
}

func (dc *DiskTableCatalog) GetTable(name string) (*Table, bool) {
	tbl, ok := dc.Tables[name]
	return tbl, ok
}

func main() {
	// Use a single TableCatalog with a valid WAL for all SQL execution
	catalog := NewTableCatalog()
	if _, ok := catalog.Tables["users"]; !ok {
		catalog.CreateTable("users", []string{"id", "name", "age"})
	}

	fmt.Println("Welcome to the VarmaDB CLI!")
	fmt.Println("Type SQL statements to interact with the database. Type 'exit' or 'quit' to leave.")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("varmaDB> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			continue
		}
		trimmed := strings.TrimSpace(input)
		if trimmed == "exit" || trimmed == "quit" {
			fmt.Println("Goodbye!")
			break
		}
		if trimmed == "" {
			continue
		}
		err = ExecuteSQL(catalog, trimmed)
		if err != nil {
			fmt.Println("  Error:", err)
		}
	}
}

func NewDiskBTree(pager *Pager, root uint64) *BTree {
	return &BTree{
		root: root,
		get: func(ptr uint64) []byte {
			buf, _ := pager.Get(ptr)
			return buf
		},
		new: func(buf []byte) uint64 {
			pageNum, page, _ := pager.New()
			copy(page, buf)
			return pageNum
		},
		del: func(ptr uint64) {
			pager.Del(ptr)
		},
	}
}

type BTreeIterator struct {
	tree   *BTree
	stack  []iteratorFrame
	curKey []byte
	curVal []byte
	valid  bool
}

type iteratorFrame struct {
	ptr uint64
	idx uint16
	max uint16
}

// NewBTreeIterator returns an iterator starting at the smallest key >= startKey (or first if startKey==nil)
func NewBTreeIterator(tree *BTree, startKey []byte) *BTreeIterator {
	it := &BTreeIterator{tree: tree}
	ptr := tree.root
	if ptr == 0 {
		it.valid = false
		return it
	}
	// Descend to leaf
	for {
		node := BNode(tree.get(ptr))
		n := node.nkeys()
		if node.isLeaf() {
			var idx uint16 = 1
			if startKey != nil {
				for idx = 1; idx <= n; idx++ {
					if cmp(node.getKey(idx), startKey) >= 0 {
						break
					}
				}
				if idx > n {
					it.valid = false
					return it
				}
			}
			it.stack = append(it.stack, iteratorFrame{ptr, idx, n})
			it.loadCurrent()
			return it
		}
		// Internal node: descend
		var idx uint16 = 1
		for ; idx <= n; idx++ {
			if startKey != nil && cmp(node.getKey(idx), startKey) > 0 {
				break
			}
		}
		it.stack = append(it.stack, iteratorFrame{ptr, idx, n})
		ptr = node.getPtr(idx - 1)
	}
}

func (it *BTreeIterator) loadCurrent() {
	if len(it.stack) == 0 {
		it.valid = false
		return
	}
	frame := it.stack[len(it.stack)-1]
	node := BNode(it.tree.get(frame.ptr))
	if frame.idx > frame.max {
		it.valid = false
		return
	}
	it.curKey = node.getKey(frame.idx)
	it.curVal = node.getVal(frame.idx)
	it.valid = true
}

func (it *BTreeIterator) Next() {
	if !it.valid || len(it.stack) == 0 {
		it.valid = false
		return
	}
	frame := it.stack[len(it.stack)-1]
	frame.idx++
	if frame.idx > frame.max {
		it.valid = false
		return
	}
	it.stack[len(it.stack)-1] = frame
	it.loadCurrent()
}

func (it *BTreeIterator) Key() []byte {
	return it.curKey
}

func (it *BTreeIterator) Value() []byte {
	return it.curVal
}

func (it *BTreeIterator) Valid() bool {
	return it.valid
}
