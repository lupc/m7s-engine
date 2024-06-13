package util

import (
	"sync/atomic"
)

type emptyLocker struct{}

func (emptyLocker) Lock()   {}
func (emptyLocker) Unlock() {}

var EmptyLocker emptyLocker

type IDataFrame[T any] interface {
	Init()                // 初始化
	Reset()               // 重置数据,复用内存
	Ready()               // 标记为可读取
	ReaderEnter()         // 读取者数量+1
	ReaderTryEnter() bool // 尝试读取
	ReaderLeave()         // 读取者数量-1
	StartWrite() bool     // 开始写入
	SetSequence(uint32)   // 设置序号
	GetSequence() uint32  // 获取序号
	IsDiscarded() bool    // 是否已废弃
}

type RingWriter[T any, F IDataFrame[T]] struct {
	*Ring[F]    `json:"-" yaml:"-"`
	ReaderCount atomic.Int32 `json:"-" yaml:"-"`
	pool        *Ring[F]
	poolSize    int
	Size        int
	LastValue   F
	constructor func() F
	disposeFlag atomic.Int32
}

func (rb *RingWriter[T, F]) create(n int) (ring *Ring[F]) {
	ring = NewRing[F](n)
	for p, i := ring, n; i > 0; p, i = p.Next(), i-1 {
		p.Value = rb.constructor()
		p.Value.Init()
	}
	return
}

func (rb *RingWriter[T, F]) Init(n int, constructor func() F) *RingWriter[T, F] {
	rb.constructor = constructor
	rb.Ring = rb.create(n)
	rb.Size = n
	rb.LastValue = rb.Value
	rb.Value.StartWrite()
	return rb
}

func (rb *RingWriter[T, F]) Glow(size int) (newItem *Ring[F]) {
	if size < rb.poolSize {
		newItem = rb.pool.Unlink(size)
		rb.poolSize -= size
	} else if size == rb.poolSize {
		newItem = rb.pool
		rb.poolSize = 0
		rb.pool = nil
	} else {
		newItem = rb.create(size - rb.poolSize).Link(rb.pool)
		rb.poolSize = 0
		rb.pool = nil
	}
	rb.Link(newItem)
	rb.Size += size
	return
}

func (rb *RingWriter[T, F]) recycle(r *Ring[F]) {
	rb.poolSize++
	r.Value.Init()
	r.Value.Reset()
	if rb.pool == nil {
		rb.pool = r
	} else {
		rb.pool.Link(r)
	}
}

func (rb *RingWriter[T, F]) Reduce(size int) {
	r := rb.Unlink(size)
	for p := r.Next(); p != r; {
		next := p.Next() //先保存下一个节点
		if p.Value.StartWrite() {
			p.Value.Ready()
			rb.recycle(p)
		} else {
			p.Prev().Unlink(1).Value.Reset()
		}
		p = next
	}
	rb.Size -= size
}

func (rb *RingWriter[T, F]) Dispose() {
	if rb.disposeFlag.Add(-2) == -2 {
		rb.Value.Ready()
	}
}

func (rb *RingWriter[T, F]) Step() (normal bool) {
	if !rb.disposeFlag.CompareAndSwap(0, 1) {
		// already disposed
		return
	}
	rb.LastValue = rb.Value
	nextSeq := rb.LastValue.GetSequence() + 1
	next := rb.Next()
	if normal = next.Value.StartWrite(); normal {
		next.Value.Reset()
		rb.Ring = next
	} else {
		rb.Reduce(1)         //抛弃还有订阅者的节点
		rb.Ring = rb.Glow(1) //补充一个新节点
		if !rb.Value.StartWrite() {
			panic("can't start write")
		}
	}
	rb.Value.SetSequence(nextSeq)
	rb.LastValue.Ready()
	if !rb.disposeFlag.CompareAndSwap(1, 0) {
		rb.Value.Ready()
	}
	return
}

func (rb *RingWriter[T, F]) GetReaderCount() int32 {
	return rb.ReaderCount.Load()
}
