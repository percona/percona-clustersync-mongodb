package list

import "iter"

// List is a doubly linked list.
type List[T any] struct {
	head *listElem[T]
	tail *listElem[T]
}

// listElem is an element in the doubly linked list.
type listElem[T any] struct {
	prev *listElem[T]
	next *listElem[T]
	val  T
}

// Push adds a new element to the end of the list.
func (l *List[T]) Push(val T) {
	elem := &listElem[T]{val: val}

	if l.head == nil {
		l.head = elem
		l.tail = elem
		return
	}

	l.tail.next = elem
	elem.prev = l.tail
	l.tail = elem
}

// PushFront adds a new element to the front of the list.
func (l *List[T]) PushFront(val T) {
	elem := &listElem[T]{val: val}

	if l.head == nil {
		l.head = elem
		l.tail = elem
		return
	}

	elem.next = l.head
	l.head.prev = elem
	l.head = elem
}

// Pop removes and returns the first element from the list.
func (l *List[T]) Pop() (T, bool) { //nolint:ireturn
	if l.head == nil {
		var zero T
		return zero, false
	}
	val := l.head.val
	l.head = l.head.next
	if l.head != nil {
		l.head.prev = nil
	} else {
		l.tail = nil
	}
	return val, true
}

// PopFront removes and returns the first element from the list.
func (l *List[T]) PopFront() (T, bool) { //nolint:ireturn
	return l.Pop()
}

// IsEmpty checks if the list is empty.
func (l *List[T]) IsEmpty() bool {
	return l.head == nil
}

// Clear removes all elements from the list.
func (l *List[T]) Clear() {
	l.head = nil
	l.tail = nil
}

// All returns an iterator for all elements in the list.
func (l *List[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for e := l.head; e != nil; e = e.next {
			if !yield(e.val) {
				return
			}
		}
	}
}

// Backward returns an iterator for all elements in the list in reverse order.
func (l *List[T]) Backward() iter.Seq[T] {
	return func(yield func(T) bool) {
		for e := l.tail; e != nil; e = e.prev {
			if !yield(e.val) {
				return
			}
		}
	}
}
