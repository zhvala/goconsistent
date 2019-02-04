// Copyright (C) 2019 zhvala.
// Use of this source code is governed by an MIT-style license
// that can be found in the LICENSE file.

// Package consistent provides a consistent hashing function.
//
// Consistent hashing is often used to distribute requests to a changing set of servers.  For example,
// say you have some cache servers cacheA, cacheB, and cacheC.  You want to decide which cache server
// to use to look up information on a user.
//
// You could use a typical hash table and hash the user id
// to one of cacheA, cacheB, or cacheC.  But with a typical hash table, if you add or remove a server,
// almost all keys will get remapped to different results, which basically could bring your service
// to a grinding halt while the caches get rebuilt.
//
// With a consistent hash, adding or removing a server drastically reduces the number of keys that
// get remapped.
//
// Read more about consistent hashing on wikipedia:  http://en.wikipedia.org/wiki/Consistent_hashing
//
package consistent

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

const (
	// DefaultReplicaNumber default replica number
	DefaultReplicaNumber = 20
)

type uints []uint32

// Len returns the length of the uints array.
func (x uints) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x uints) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x uints) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// ErrEmptyCircle is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyCircle = errors.New("empty circle")

// Element contains key、value and replica
type Element struct {
	Key     string
	Value   interface{}
	Replica int
}

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	circle           map[uint32]string
	members          map[string]*Element
	sortedHashes     uints
	NumberOfReplicas int
	count            int64
	scratch          [64]byte
	sync.RWMutex
}

// New creates a new Consistent object with a default setting of 20 replicas for each entry.
//
// To change the number of replicas, set NumberOfReplicas before adding entries.
func New() *Consistent {
	c := new(Consistent)
	c.NumberOfReplicas = DefaultReplicaNumber
	c.circle = make(map[uint32]string)
	c.members = make(map[string]*Element)
	c.sortedHashes = make(uints, 0, 1024)
	return c
}

// eltKey generates a string key for an element with an index.
func (c *Consistent) eltKey(elt string, idx int) string {
	// return elt + "|" + strconv.Itoa(idx)
	return strconv.Itoa(idx) + elt
}

// Add inserts a element in the consistent hash.
func (c *Consistent) Add(key string, value interface{}) {
	c.AddReplicas(key, value, c.NumberOfReplicas)
}

// AddReplicas inserts a element with replica number in the consistent hash.
func (c *Consistent) AddReplicas(key string, value interface{}, replica int) {
	c.Lock()
	defer c.Unlock()
	c.add(key, value, replica)
}

// need c.Lock() before calling
func (c *Consistent) add(key string, value interface{}, replica int) {
	for i := 0; i < replica; i++ {
		c.circle[c.hashKey(c.eltKey(key, i))] = key
	}
	c.members[key] = &Element{key, value, replica}
	c.updateSortedHashes()
	c.count++
}

// Remove removes an element from the hash.
func (c *Consistent) Remove(key string) {
	c.Lock()
	defer c.Unlock()
	c.remove(key)
}

// need c.Lock() before calling
func (c *Consistent) remove(key string) {
	if _, ok := c.members[key]; ok {
		for i := 0; i < c.members[key].Replica; i++ {
			delete(c.circle, c.hashKey(c.eltKey(key, i)))
		}
		delete(c.members, key)
		c.updateSortedHashes()
		c.count--
	}
}

// Set sets all the elements in the hash.  If there are existing elements not
// present in elts, they will be removed.
func (c *Consistent) Set(kvs map[string]interface{}) {
	c.Lock()
	defer c.Unlock()
	for key := range c.members {
		c.remove(key)
	}

	for k, v := range kvs {
		c.add(k, v, c.NumberOfReplicas)
	}
}

// Members return all members in consistent hash.
func (c *Consistent) Members() map[string]interface{} {
	c.RLock()
	defer c.RUnlock()
	members := make(map[string]interface{}, len(c.members))
	for k, v := range c.members {
		members[k] = v
	}
	return members
}

// Get returns an element close to where name hashes to in the circle.
func (c *Consistent) Get(raw string) (*Element, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return nil, ErrEmptyCircle
	}
	key := c.hashKey(raw)
	i := c.search(key)
	return c.members[c.circle[c.sortedHashes[i]]], nil
}

func (c *Consistent) search(key uint32) (i int) {
	f := func(x int) bool {
		return c.sortedHashes[x] > key
	}
	i = sort.Search(len(c.sortedHashes), f)
	if i >= len(c.sortedHashes) {
		i = 0
	}
	return
}

// GetTwo returns the two closest distinct elements to the name input in the circle.
func (c *Consistent) GetTwo(name string) (*Element, *Element, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return nil, nil, ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	first := c.members[c.circle[c.sortedHashes[i]]]

	if c.count == 1 {
		return first, nil, nil
	}

	start := i
	var second *Element
	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		second = c.members[c.circle[c.sortedHashes[i]]]
		if second != first {
			break
		}
	}
	return first, second, nil
}

// GetN returns the N closest distinct elements to the name input in the circle.
func (c *Consistent) GetN(name string, n int) ([]*Element, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return nil, ErrEmptyCircle
	}

	if c.count < int64(n) {
		n = int(c.count)
	}

	var (
		key   = c.hashKey(name)
		i     = c.search(key)
		start = i
		res   = make([]*Element, 0, n)
		elem  = c.members[c.circle[c.sortedHashes[i]]]
	)

	res = append(res, elem)

	if len(res) == n {
		return res, nil
	}

	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		elem = c.members[c.circle[c.sortedHashes[i]]]
		if !sliceContainsMember(res, elem) {
			res = append(res, elem)
		}
		if len(res) == n {
			break
		}
	}

	return res, nil
}

func (c *Consistent) hashKey(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	for k := range c.circle {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}

func sliceContainsMember(set []*Element, member *Element) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}
