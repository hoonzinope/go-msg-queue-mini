package internal

import (
	"container/list"
	"sync"
)

type Queue struct {
	items *list.List
	mutex sync.Mutex
}

func NewQueue() *Queue {
	return &Queue{
		items: list.New(),
		mutex: sync.Mutex{},
	}
}

func (q *Queue) Enqueue(item interface{}) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.items.PushBack(item)
}

func (q *Queue) Dequeue() interface{} {
	q.mutex.Lock()
	defer q.mutex.Unlock()
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
