package main

import (
	"container/list"
	"io"
	"os"
	"sync"
)

const (
	PAGE_SIZE = 4096
)

type lruEntry struct {
	pageNum uint64
	buf     []byte
}

type Pager struct {
	file     *os.File
	mutex    sync.Mutex
	pages    map[uint64]*list.Element // LRU cache: pageNum -> *Element
	lru      *list.List               // LRU order: most recent front
	free     []uint64                 // free list
	npages   uint64
	maxCache int
}

func OpenPager(filename string, maxCache int) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	npages := uint64(info.Size() / PAGE_SIZE)
	return &Pager{
		file:     file,
		pages:    make(map[uint64]*list.Element),
		lru:      list.New(),
		free:     make([]uint64, 0),
		npages:   npages,
		maxCache: maxCache,
	}, nil
}

func (p *Pager) Get(pageNum uint64) ([]byte, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if elem, ok := p.pages[pageNum]; ok {
		p.lru.MoveToFront(elem)
		return elem.Value.(*lruEntry).buf, nil
	}
	buf := make([]byte, PAGE_SIZE)
	_, err := p.file.ReadAt(buf, int64(pageNum)*PAGE_SIZE)
	if err != nil && err != io.EOF {
		return nil, err
	}
	entry := &lruEntry{pageNum, buf}
	el := p.lru.PushFront(entry)
	p.pages[pageNum] = el
	if p.lru.Len() > p.maxCache {
		p.evict()
	}
	return buf, nil
}

func (p *Pager) New() (uint64, []byte, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	var pageNum uint64
	if len(p.free) > 0 {
		pageNum = p.free[len(p.free)-1]
		p.free = p.free[:len(p.free)-1]
	} else {
		pageNum = p.npages
		p.npages++
	}
	buf := make([]byte, PAGE_SIZE)
	entry := &lruEntry{pageNum, buf}
	el := p.lru.PushFront(entry)
	p.pages[pageNum] = el
	if p.lru.Len() > p.maxCache {
		p.evict()
	}
	return pageNum, buf, nil
}

func (p *Pager) Del(pageNum uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if elem, ok := p.pages[pageNum]; ok {
		p.lru.Remove(elem)
		delete(p.pages, pageNum)
	}
	p.free = append(p.free, pageNum)
}

func (p *Pager) evict() {
	// Remove least recently used page from cache
	el := p.lru.Back()
	if el == nil {
		return
	}
	entry := el.Value.(*lruEntry)
	p.Flush(entry.pageNum) // flush before evict
	delete(p.pages, entry.pageNum)
	p.lru.Remove(el)
}

func (p *Pager) Flush(pageNum uint64) error {
	if elem, ok := p.pages[pageNum]; ok {
		buf := elem.Value.(*lruEntry).buf
		_, err := p.file.WriteAt(buf, int64(pageNum)*PAGE_SIZE)
		return err
	}
	return nil
}

func (p *Pager) FlushAll() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for pageNum := range p.pages {
		if err := p.Flush(pageNum); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pager) Close() error {
	p.FlushAll()
	return p.file.Close()
}

// Recovery logic: reload free list from a special page (not implemented, placeholder)
func (p *Pager) Recover() error {
	// In a real system, would read a metadata page to restore free list, etc.
	return nil
}
