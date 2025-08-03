package internal

import (
	"container/list"
	"sync"
)

type Queue struct {
	items *list.List
}

var mutex = &sync.Mutex{}

func NewQueue() *Queue {
	return &Queue{
		items: list.New(),
	}
}

func (q *Queue) Enqueue(item interface{}) {
	mutex.Lock()
	defer mutex.Unlock()
	q.items.PushBack(item)
}

func (q *Queue) Dequeue() interface{} {
	mutex.Lock()
	defer mutex.Unlock()
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
