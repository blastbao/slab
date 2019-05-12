package slab

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// AtomPool is a lock-free slab allocation memory pool.
type AtomPool struct {
	classes []class
	minSize int
	maxSize int
}

// NewAtomPool create a lock-free slab allocation memory pool.
// minSize is the smallest chunk size.
// maxSize is the lagest chunk size.
// factor is used to control growth of chunk size.
// pageSize is the memory size of each slab class.
func NewAtomPool(minSize, maxSize, factor, pageSize int) *AtomPool {

	pool := &AtomPool{
		make([]class, 0, 10), // 每种 class 对应一种大小的 chunk
		minSize,              // 最小 chunk 的大小
		maxSize,              // 最大 chunk 的大小
	}

	// 为每种大小的 chunk: minSize, minSize * factor, minSize * factor * factor, ... , maxSize 创建一个 class 
	for chunkSize := minSize; chunkSize <= maxSize && chunkSize <= pageSize; chunkSize *= factor {
		
		// 为每种 chunkSize 大小的 chunk 创建一个 class 
		c := class{
			size:   chunkSize,
			page:   make([]byte, pageSize),             // 每个 class 的总大小为 pageSize，默认 64KB
			chunks: make([]chunk, pageSize/chunkSize),  // 每个 class 包含的 chunk 总数为 pageSize/chunkSize 个
			head:   (1 << 32),                          // ???
		}

		// 初始化 class 中所含的 chunks
		for i := 0; i < len(c.chunks); i++ {

			chk := &c.chunks[i]

			// 把字节数组 c.page 按序切分成一个个 chunk，起始地址保存到变量 chk.mem 上
			chk.mem = c.page[i*chunkSize : (i+1)*chunkSize : (i+1)*chunkSize] // lock down the capacity to protect append operation
			
			// 如果是最后一个 chunk，
			if i < len(c.chunks)-1 {
				chk.next = uint64(i+1+1 /* index start from 1 */) << 32
			} else {
				c.pageBegin = uintptr(unsafe.Pointer(&c.page[0]))
				c.pageEnd = uintptr(unsafe.Pointer(&chk.mem[0]))
			}


		}

		pool.classes = append(pool.classes, c)
	}
	return pool
}

// Alloc try alloc a []byte from internal slab class if no free chunk in slab class Alloc will make one.
func (pool *AtomPool) Alloc(size int) []byte {
	if size <= pool.maxSize {
		for i := 0; i < len(pool.classes); i++ {
			if pool.classes[i].size >= size {
				mem := pool.classes[i].Pop()
				if mem != nil {
					return mem[:size]
				}
				break
			}
		}
	}
	return make([]byte, size)
}

// Free release a []byte that alloc from Pool.Alloc.
func (pool *AtomPool) Free(mem []byte) {
	size := cap(mem)
	for i := 0; i < len(pool.classes); i++ {
		if pool.classes[i].size == size {
			pool.classes[i].Push(mem)
			break
		}
	}
}

type class struct {
	size      int
	page      []byte
	pageBegin uintptr
	pageEnd   uintptr
	chunks    []chunk
	head      uint64
}

type chunk struct {
	mem  []byte
	aba  uint32 // reslove ABA problem
	next uint64
}

func (c *class) Push(mem []byte) {

	// 获取切片 mem 的底层数组的首指针 ptr
	ptr := (*reflect.SliceHeader)(unsafe.Pointer(&mem)).Data

	// 判断 ptr 是否属于本 class 管辖的内存范围，若属于则进行回收，否则不予处理
	if c.pageBegin <= ptr && ptr <= c.pageEnd {

		// 计算 ptr 属于当前 class 内的第几个 chunk 
		i := (ptr - c.pageBegin) / uintptr(c.size)

		// 取出 ptr 所属 chunk
		chk := &c.chunks[i]

		// 已分配的 chunk 的 chk.next 值应为 0，若非 0，则意味着此前已被回收，报错
		if chk.next != 0 {
			panic("slab.AtomPool: Double Free")
		}

		chk.aba++

		// 被回收的 chunk 放到 class 空闲链表首部，因此：
		// 
		// chk := &c.chunks[i]
		// chk.next = c.head
		// c.head = i
		// 
		// 备注，这里第三步的 i 实际上是 new = f(i) = uint64(i+1)<<32 + uint64(chk.aba++)
		new := uint64(i+1)<<32 + uint64(chk.aba)

		for {
			// 相当于 chk.next = c.head
			old := atomic.LoadUint64(&c.head)
			atomic.StoreUint64(&chk.next, old)
			// 相当于 c.head = i
			if atomic.CompareAndSwapUint64(&c.head, old, new) {
				break
			}
			runtime.Gosched()
		}

	}

}

func (c *class) Pop() []byte {

	// 从本 class 空闲链表推出首部 chunk :
	// 
	// chk := &c.chunks[c.head] // 取出首元素
	// c.head = chk.next        // 更新首指针
	// chk.next = 0             // 重置取出元素的next指针
	// return chk.mem           // 返回已取出的首元素
	// 
	for {

		// 获取当前 class 的空闲列表的首 chunk 的下标
		old := atomic.LoadUint64(&c.head)
		if old == 0 {
			return nil
		}

		// 取出 head 对应的 chunk: chk, 同时取出其下个 chunk 的坐标: nxt
		chk := &c.chunks[old>>32-1]
		nxt := atomic.LoadUint64(&chk.next)

		// 把 nxt 设置为当前 class 的空闲列表的首 chunk 下标
		if atomic.CompareAndSwapUint64(&c.head, old, nxt) {
			// 把 chk 的 next 指针置零
			atomic.StoreUint64(&chk.next, 0)
			// 返回 chk.mem
			return chk.mem
		}

		runtime.Gosched()
	}
}
