package main

import (
	"os"
	"sync"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Append(entry []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.file.Write(append(entry, '\n'))
	return err
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Replay reads all log entries and returns them as a slice of byte slices
func (w *WAL) Replay() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.file.Seek(0, 0)
	var entries [][]byte
	buf := make([]byte, 4096)
	var line []byte
	for {
		n, err := w.file.Read(buf)
		if n == 0 || err != nil {
			break
		}
		for _, b := range buf[:n] {
			if b == '\n' {
				entries = append(entries, append([]byte{}, line...))
				line = line[:0]
			} else {
				line = append(line, b)
			}
		}
	}
	return entries, nil
}
