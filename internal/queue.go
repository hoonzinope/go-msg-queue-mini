package internal

import (
	"container/list"
	"sync"
)

type Queue struct {
	items     *list.List
	mutex     sync.Mutex
	msgSignal chan interface{} // Channel to signal new messages
}

func NewQueue() *Queue {
	return &Queue{
		items:     list.New(),
		msgSignal: make(chan interface{}, 1),
		mutex:     sync.Mutex{},
	}
}

func (q *Queue) Enqueue(item interface{}) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.items.PushBack(item)
	select {
	case q.msgSignal <- item: // Signal for consumers that a new message is available
	default:
		// If the channel is full, drop the signal
		return
	}
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
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.items.Len() == 0
}

func (q *Queue) Length() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.items.Len()
}
