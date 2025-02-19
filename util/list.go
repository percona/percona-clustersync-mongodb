package util

import "iter"

type List[T any] struct {
	head *listElem[T]
	tail *listElem[T]
}

type listElem[T any] struct {
	next *listElem[T]
	val  T
}

func (l *List[T]) Push(val T) {
	elem := &listElem[T]{val: val}

	if l.head == nil {
		l.head = elem
		l.tail = elem
		return
	}

	l.tail.next = elem
	l.tail = elem
}

func (l *List[T]) Clear() {
	l.head = nil
	l.tail = nil
}

func (l *List[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for e := l.head; e != nil; e = e.next {
			if !yield(e.val) {
				return
			}
		}
	}
}
