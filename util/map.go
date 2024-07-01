package util

import (
	"sort"
	"sync"
)

type Map[K comparable, V any] struct {
	sync.Map
}

func (m *Map[K, V]) Add(k K, v V) bool {
	_, loaded := m.LoadOrStore(k, v)
	return !loaded
}

func (m *Map[K, V]) Set(k K, v V) {
	m.Store(k, v)
}

func (m *Map[K, V]) Has(k K) (ok bool) {
	_, ok = m.Load(k)
	return
}

func (m *Map[K, V]) Len() (l int) {
	m.Map.Range(func(k, v interface{}) bool {
		l++
		return true
	})
	return
}

func (m *Map[K, V]) Get(k K) (result V) {
	v, ok := m.Load(k)
	if !ok {
		return
	}
	return v.(V)
}

func (m *Map[K, V]) Delete(k K) (v V, ok bool) {
	var r any
	if r, ok = m.Map.LoadAndDelete(k); ok {
		v = r.(V)
	}
	return
}

func (m *Map[K, V]) ToList() (r []V) {
	m.Map.Range(func(k, v interface{}) bool {
		r = append(r, v.(V))
		return true
	})
	return
}

func MapList[K comparable, V any, R any](m *Map[K, V], f func(K, V) R) (r []R) {
	m.Map.Range(func(k, v interface{}) bool {
		r = append(r, f(k.(K), v.(V)))
		return true
	})
	return
}

func (m *Map[K, V]) Range(f func(K, V)) {
	m.Map.Range(func(k, v interface{}) bool {
		f(k.(K), v.(V))
		return true
	})
}

func (m *Map[K, V]) RangeSorted(f func(K, V), compare func(K, K) bool) {
	// Collect keys and values
	var pairs []struct {
		key K
		val V
	}
	m.Map.Range(func(k, v interface{}) bool {
		pairs = append(pairs, struct {
			key K
			val V
		}{k.(K), v.(V)})
		return true
	})

	// Sort by keys using the compare function
	sort.Slice(pairs, func(i, j int) bool {
		return compare(pairs[i].key, pairs[j].key)
	})

	// Apply function in sorted order
	for _, pair := range pairs {
		f(pair.key, pair.val)
	}
}
