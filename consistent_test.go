// Copyright (C) 2019 zhvala.
// Use of this source code is governed by an MIT-style license
// that can be found in the LICENSE file.

package consistent

import (
	"bufio"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

func checkNum(num, expected int, t *testing.T) {
	if num != expected {
		t.Errorf("got %d, expected %d", num, expected)
	}
}

func TestNew(t *testing.T) {
	x := New()
	if x == nil {
		t.Errorf("expected obj")
	}
	checkNum(x.NumberOfReplicas, 20, t)
}

func TestAdd(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	checkNum(len(x.circle), 20, t)
	checkNum(len(x.sortedHashes), 20, t)
	if sort.IsSorted(x.sortedHashes) == false {
		t.Errorf("expected sorted hashes to be sorted")
	}
	x.Add("qwer", "value2")
	checkNum(len(x.circle), 40, t)
	checkNum(len(x.sortedHashes), 40, t)
	if sort.IsSorted(x.sortedHashes) == false {
		t.Errorf("expected sorted hashes to be sorted")
	}
}

func TestRemove(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Remove("abcdefg")
	checkNum(len(x.circle), 0, t)
	checkNum(len(x.sortedHashes), 0, t)
}

func TestRemoveNonExisting(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Remove("abcdefghijk")
	checkNum(len(x.circle), 20, t)
}

func TestGetEmpty(t *testing.T) {
	x := New()
	_, err := x.Get("asdfsadfsadf")
	if err == nil {
		t.Errorf("expected error")
	}
	if err != ErrEmptyCircle {
		t.Errorf("expected empty circle error")
	}
}

func TestGetSingle(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	f := func(s string) bool {
		y, err := x.Get(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		t.Logf("s = %q, y = %q", s, y)
		return y.Key == "abcdefg"
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

type gtest struct {
	in  string
	out string
}

var gmtests = []gtest{
	{"ggg", "abcdefg"},
	{"hhh", "opqrstu"},
	{"iiiii", "hijklmn"},
}

func TestGetMultiple(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	for i, v := range gmtests {
		result, err := x.Get(v.in)
		if err != nil {
			t.Fatal(err)
		}
		if result.Key != v.out {
			t.Errorf("%d. got %q, expected %q", i, result, v.out)
		}
	}
}

func TestGetMultipleQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	f := func(s string) bool {
		y, err := x.Get(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		t.Logf("s = %q, y = %q", s, y)
		return y.Key == "abcdefg" || y.Key == "hijklmn" || y.Key == "opqrstu"
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

var rtestsBefore = []gtest{
	{"ggg", "abcdefg"},
	{"hhh", "opqrstu"},
	{"iiiii", "hijklmn"},
}

var rtestsAfter = []gtest{
	{"ggg", "abcdefg"},
	{"hhh", "opqrstu"},
	{"iiiii", "opqrstu"},
}

func TestGetMultipleRemove(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	for i, v := range rtestsBefore {
		result, err := x.Get(v.in)
		if err != nil {
			t.Fatal(err)
		}
		if result.Key != v.out {
			t.Errorf("%d. got %q, expected %q before rm", i, result, v.out)
		}
	}
	x.Remove("hijklmn")
	for i, v := range rtestsAfter {
		result, err := x.Get(v.in)
		if err != nil {
			t.Fatal(err)
		}
		if result.Key != v.out {
			t.Errorf("%d. got %q, expected %q after rm", i, result, v.out)
		}
	}
}

func TestGetMultipleRemoveQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	x.Remove("opqrstu")
	f := func(s string) bool {
		y, err := x.Get(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		t.Logf("s = %q, y = %q", s, y)
		return y.Key == "abcdefg" || y.Key == "hijklmn"
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetTwo(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	a, b, err := x.GetTwo("99999999")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Errorf("a shouldn't equal b")
	}
	if a.Key != "abcdefg" {
		t.Errorf("wrong a: %q", a.Key)
	}
	if b.Key != "hijklmn" {
		t.Errorf("wrong b: %q", b.Key)
	}
}

func TestGetTwoQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	f := func(s string) bool {
		a, b, err := x.GetTwo(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		if a == b {
			t.Logf("a == b")
			return false
		}
		if a.Key != "abcdefg" && a.Key != "hijklmn" && a.Key != "opqrstu" {
			t.Logf("invalid a: %q", a.Key)
			return false
		}

		if b.Key != "abcdefg" && b.Key != "hijklmn" && b.Key != "opqrstu" {
			t.Logf("invalid b: %q", b.Key)
			return false
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetTwoOnlyTwoQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	f := func(s string) bool {
		a, b, err := x.GetTwo(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		if a == b {
			t.Logf("a == b")
			return false
		}
		if a.Key != "abcdefg" && a.Key != "hijklmn" {
			t.Logf("invalid a: %q", a.Key)
			return false
		}

		if b.Key != "abcdefg" && b.Key != "hijklmn" {
			t.Logf("invalid b: %q", b.Key)
			return false
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetTwoOnlyOneInCircle(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	a, b, err := x.GetTwo("99999999")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Errorf("a shouldn't equal b")
	}
	if a == nil || a.Key != "abcdefg" {
		t.Errorf("wrong a")
	}
	if b != nil {
		t.Errorf("wrong b")
	}
}

func TestGetN(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	members, err := x.GetN("9999999", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 {
		t.Errorf("expected 3 members instead of %d", len(members))
	}
	if members[0].Key != "opqrstu" {
		t.Errorf("wrong members[0].Key: %q", members[0].Key)
	}
	if members[1].Key != "abcdefg" {
		t.Errorf("wrong members[1].Key: %q", members[1].Key)
	}
	if members[2].Key != "hijklmn" {
		t.Errorf("wrong members[2].Key: %q", members[2].Key)
	}
}

func TestGetNLess(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	members, err := x.GetN("99999999", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Errorf("expected 2 members instead of %d", len(members))
	}
	if members[0].Key != "abcdefg" {
		t.Errorf("wrong members[0].Key: %q", members[0].Key)
	}
	if members[1].Key != "hijklmn" {
		t.Errorf("wrong members[1].Key: %q", members[1].Key)
	}
}

func TestGetNMore(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	members, err := x.GetN("9999999", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 {
		t.Errorf("expected 3 members instead of %d", len(members))
	}
	if members[0].Key != "opqrstu" {
		t.Errorf("wrong members[0].Key: %q", members[0].Key)
	}
	if members[1].Key != "abcdefg" {
		t.Errorf("wrong members[1].Key: %q", members[1].Key)
	}
	if members[2].Key != "hijklmn" {
		t.Errorf("wrong members[2].Key: %q", members[2].Key)
	}
}

func TestGetNQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	f := func(s string) bool {
		members, err := x.GetN(s, 3)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		if len(members) != 3 {
			t.Logf("expected 3 members instead of %d", len(members))
			return false
		}
		set := make(map[*Element]bool, 4)
		for _, member := range members {
			if set[member] {
				t.Logf("duplicate error")
				return false
			}
			set[member] = true
			if member.Key != "abcdefg" && member.Key != "hijklmn" && member.Key != "opqrstu" {
				t.Logf("invalid member: %q", member)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetNLessQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	f := func(s string) bool {
		members, err := x.GetN(s, 2)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		if len(members) != 2 {
			t.Logf("expected 2 members instead of %d", len(members))
			return false
		}
		set := make(map[*Element]bool, 4)
		for _, member := range members {
			if set[member] {
				t.Logf("duplicate error")
				return false
			}
			set[member] = true
			if member.Key != "abcdefg" && member.Key != "hijklmn" && member.Key != "opqrstu" {
				t.Logf("invalid member: %q", member)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetNMoreQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg", "value1")
	x.Add("hijklmn", "value2")
	x.Add("opqrstu", "value3")
	f := func(s string) bool {
		members, err := x.GetN(s, 5)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		if len(members) != 3 {
			t.Logf("expected 3 members instead of %d", len(members))
			return false
		}
		set := make(map[*Element]bool, 4)
		for _, member := range members {
			if set[member] {
				t.Logf("duplicate error")
				return false
			}
			set[member] = true
			if member.Key != "abcdefg" && member.Key != "hijklmn" && member.Key != "opqrstu" {
				t.Logf("invalid member: %q", member)
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSet(t *testing.T) {
	x := New()
	x.Add("abc", "value-abc")
	x.Add("def", "value-def")
	x.Add("ghi", "value-ghi")
	x.Set(map[string]interface{}{"jkl": "value-jkl", "mno": "value-mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err := x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a.Key != "jkl" && a.Key != "mno" {
		t.Errorf("expected jkl or mno, got %s", a.Key)
	}
	if b.Key != "jkl" && b.Key != "mno" {
		t.Errorf("expected jkl or mno, got %s", b.Key)
	}
	if a == b {
		t.Errorf("expected a.Key != b, they were both %s", a.Key)
	}
	x.Set(map[string]interface{}{"pqr": "value-pqr", "mno": "value-mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err = x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a.Key != "pqr" && a.Key != "mno" {
		t.Errorf("expected jkl or mno, got %s", a.Key)
	}
	if b.Key != "pqr" && b.Key != "mno" {
		t.Errorf("expected jkl or mno, got %s", b.Key)
	}
	if a == b {
		t.Errorf("expected a.Key != b, they were both %s", a.Key)
	}
	x.Set(map[string]interface{}{"pqr": "value-pqr", "mno": "value-mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err = x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a.Key != "pqr" && a.Key != "mno" {
		t.Errorf("expected jkl or mno, got %s", a.Key)
	}
	if b.Key != "pqr" && b.Key != "mno" {
		t.Errorf("expected jkl or mno, got %s", b.Key)
	}
	if a == b {
		t.Errorf("expected a.Key != b, they were both %s", a.Key)
	}
}

// allocBytes returns the number of bytes allocated by invoking f.
func allocBytes(f func()) uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	t := stats.TotalAlloc
	f()
	runtime.ReadMemStats(&stats)
	return stats.TotalAlloc - t
}

func mallocNum(f func()) uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	t := stats.Mallocs
	f()
	runtime.ReadMemStats(&stats)
	return stats.Mallocs - t
}

func BenchmarkAllocations(b *testing.B) {
	x := New()
	x.Add("stays", "value-stays")
	b.ResetTimer()
	allocSize := allocBytes(func() {
		for i := 0; i < b.N; i++ {
			x.Add("Foo", "value-Foo")
			x.Remove("Foo")
		}
	})
	b.Logf("%d: Allocated %d bytes (%.2fx)", b.N, allocSize, float64(allocSize)/float64(b.N))
}

func BenchmarkMalloc(b *testing.B) {
	x := New()
	x.Add("stays", "value-stays")
	b.ResetTimer()
	mallocs := mallocNum(func() {
		for i := 0; i < b.N; i++ {
			x.Add("Foo", "value-Foo")
			x.Remove("Foo")
		}
	})
	b.Logf("%d: Mallocd %d times (%.2fx)", b.N, mallocs, float64(mallocs)/float64(b.N))
}

func BenchmarkCycle(b *testing.B) {
	x := New()
	x.Add("nothing", "")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Add("foo"+strconv.Itoa(i), "")
		x.Remove("foo" + strconv.Itoa(i))
	}
}

func BenchmarkCycleLarge(b *testing.B) {
	x := New()
	for i := 0; i < 10; i++ {
		x.Add("start"+strconv.Itoa(i), "")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Add("foo"+strconv.Itoa(i), "")
		x.Remove("foo" + strconv.Itoa(i))
	}
}

func BenchmarkGet(b *testing.B) {
	x := New()
	x.Add("nothing", "")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Get("nothing")
	}
}

func BenchmarkGetLarge(b *testing.B) {
	x := New()
	for i := 0; i < 10; i++ {
		x.Add("start"+strconv.Itoa(i), "")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Get("nothing")
	}
}

func BenchmarkGetN(b *testing.B) {
	x := New()
	x.Add("nothing", "")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.GetN("nothing", 3)
	}
}

func BenchmarkGetNLarge(b *testing.B) {
	x := New()
	for i := 0; i < 10; i++ {
		x.Add("start"+strconv.Itoa(i), "")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.GetN("nothing", 3)
	}
}

func BenchmarkGetTwo(b *testing.B) {
	x := New()
	x.Add("nothing", "")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.GetTwo("nothing")
	}
}

func BenchmarkGetTwoLarge(b *testing.B) {
	x := New()
	for i := 0; i < 10; i++ {
		x.Add("start"+strconv.Itoa(i), "")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.GetTwo("nothing")
	}
}

// from @edsrzf on github:
func TestAddCollision(t *testing.T) {
	// These two strings produce several crc32 collisions after "|i" is
	// appended added by Consistent.eltKey.
	const s1 = "abear"
	const s2 = "solidiform"
	x := New()
	x.Add(s1, "")
	x.Add(s2, "")
	elt1, err := x.Get("abear")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	y := New()
	// add elements in opposite order
	y.Add(s2, "")
	y.Add(s1, "")
	elt2, err := y.Get(s1)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	if elt1.Key != elt2.Key {
		t.Error(elt1, "and", elt2, "should be equal")
	}
}

// inspired by @or-else on github
func TestCollisionsCRC(t *testing.T) {
	t.SkipNow()
	c := New()
	f, err := os.Open("/usr/share/dict/words")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	found := make(map[uint32]string)
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		word := scanner.Text()
		for i := 0; i < c.NumberOfReplicas; i++ {
			ekey := c.eltKey(word, i)
			// ekey := word + "|" + strconv.Itoa(i)
			k := c.hashKey(ekey)
			exist, ok := found[k]
			if ok {
				t.Logf("found collision: %s, %s", ekey, exist)
				count++
			} else {
				found[k] = ekey
			}
		}
	}
	t.Logf("number of collisions: %d", count)
}

func TestConcurrentGetSet(t *testing.T) {
	x := New()
	x.Set(map[string]interface{}{"abc": "abc", "def": "def", "ghi": "ghi", "jkl": "jkl", "mno": "mno"})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i++ {
				x.Set(map[string]interface{}{"abc": "abc", "def": "def", "ghi": "ghi", "jkl": "jkl", "mno": "mno"})
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				x.Set(map[string]interface{}{"pqr": "pqr", "stu": "stu", "vwx": "vwx"})
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			}
			wg.Done()
		}()
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i++ {
				a, err := x.Get("xxxxxxx")
				if err != nil {
					t.Error(err)
				}
				if a.Key != "def" && a.Key != "vwx" {
					t.Errorf("got %s, expected abc", a.Key)
				}
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
