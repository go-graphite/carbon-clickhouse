package receiver

func ParseBufferPlain(b *Buffer) {

	BufferPool.Put(b)
}
