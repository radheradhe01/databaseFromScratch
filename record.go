package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Simple record: [uint32 id][string name][uint32 age]
type Record struct {
	ID   uint32
	Name string
	Age  uint32
}

// Serialize a Record to bytes
func SerializeRecord(r *Record) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, r.ID)
	nameBytes := []byte(r.Name)
	binary.Write(buf, binary.LittleEndian, uint32(len(nameBytes)))
	buf.Write(nameBytes)
	binary.Write(buf, binary.LittleEndian, r.Age)
	return buf.Bytes()
}

// Deserialize bytes to a Record
func DeserializeRecord(data []byte) (*Record, error) {
	r := &Record{}
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, &r.ID); err != nil {
		return nil, err
	}
	var nameLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		return nil, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := buf.Read(nameBytes); err != nil {
		return nil, err
	}
	r.Name = string(nameBytes)
	if err := binary.Read(buf, binary.LittleEndian, &r.Age); err != nil {
		return nil, err
	}
	return r, nil
}

// For demo: print a record
func (r *Record) String() string {
	return fmt.Sprintf("Record{ID: %d, Name: %s, Age: %d}", r.ID, r.Name, r.Age)
}

// Convert map to serialized row bytes (generic, supports any schema)
func MapToRow(m map[string]string, schema []string) ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, col := range schema {
		val, ok := m[col]
		if !ok {
			val = "" // Allow missing fields, store as empty string
		}
		valBytes := []byte(val)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(valBytes))); err != nil {
			return nil, err
		}
		buf.Write(valBytes)
	}
	return buf.Bytes(), nil
}

// Convert serialized row bytes to map (generic, supports any schema)
func RowToMap(data []byte, schema []string) (map[string]string, error) {
	m := make(map[string]string)
	buf := bytes.NewReader(data)
	for _, col := range schema {
		var l uint32
		if err := binary.Read(buf, binary.LittleEndian, &l); err != nil {
			return nil, err
		}
		valBytes := make([]byte, l)
		if _, err := buf.Read(valBytes); err != nil {
			return nil, err
		}
		m[col] = string(valBytes)
	}
	return m, nil
}
