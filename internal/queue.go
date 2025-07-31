package internal

import (
	"container/list"
)

type Queue struct {
	items *list.List
}

func NewQueue() *Queue {
	return &Queue{
		items: list.New(),
	}
}

func (q *Queue) Enqueue(item interface{}) {
	q.items.PushBack(item)
}

func (q *Queue) Dequeue() interface{} {
	if q.items.Len() == 0 {
		return nil
	}
	front := q.items.Front()
	q.items.Remove(front)
	return front.Value
}

func (q *Queue) IsEmpty() bool {
	return q.items.Len() == 0
}
