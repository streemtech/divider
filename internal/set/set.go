package set

import (
	"iter"
	"sync"
)

func New[T comparable]() Set[T] {
	return Set[T]{
		m:     map[T]struct{}{},
		Mutex: &sync.Mutex{},
	}
}

type Set[T comparable] struct {
	*sync.Mutex
	m map[T]struct{}
}

// add the given data to the set.
func (s Set[T]) Add(data ...T) {
	s.Lock()
	defer s.Unlock()
	for _, v := range data {
		s.m[v] = struct{}{}
	}
}

// remove the provided data from the set.
func (s Set[T]) Remove(data ...T) {
	s.Lock()
	defer s.Unlock()
	for _, v := range data {
		delete(s.m, v)
	}
}

// duplicate the set into a new set.
func (s Set[T]) Clone() Set[T] {
	s.Lock()
	defer s.Unlock()

	n := New[T]()
	for k := range s.m {
		n.m[k] = struct{}{}
	}

	return n
}

func (s Set[T]) Contains(key T) bool {
	_, ok := s.m[key]
	return ok
}

// return all elements in the set as an array.
func (s Set[T]) Array() []T {
	s.Lock()
	defer s.Unlock()

	a := make([]T, 0, len(s.m))
	for k := range s.m {
		a = append(a, k)
	}

	return a
}

func (s Set[T]) Iterator() iter.Seq[T] {
	return func(yield func(T) bool) {
		for key := range s.m {
			if !yield(key) {
				return
			}
		}
	}
}

func (s Set[T]) Len() int {
	return len(s.m)
}

// Difference returns all elements in the receiver that are NOT in the parameter.
// Technical possible source of deadlock if both sets attempt opposing differences at the same time.
func (s1 Set[T]) Difference(s2 Set[T]) (res Set[T]) {
	res = s1.Clone()

	s2.Lock()
	defer s2.Unlock()

	for k := range s2.m {
		res.Remove(k)
	}

	return res
}
