package extmem

import (
	"errors"
	"fmt"
	"os"
)

type Buffer struct {
	NumIO	uint64		// Number of IO's
	BufSize	uint64		// Buffer size
	BlkSize	uint64		// Block size
	NumAllBlk	uint64		// Number of blocks that can be kept in the buffer
	NumFreeBlk	uint64		// Number of available blocks in the buffer
	Data		[]byte		// Starting address of the buffer
}

// Free the memory used by a buffer
// GC will free the memory
func (b *Buffer) FreeBuffer() {
	b.Data = []byte{}
}

// Initialize a buffer with the specified buffer size and block size.
// If the initialization fails, the return value is nil;
// otherwise the pointer to the buffer
func NewBuffer(bufSize, blkSize uint64) (buf *Buffer) {
	buf = &Buffer{
		NumIO: 0,
		BufSize: bufSize,
		BlkSize: blkSize,
		NumAllBlk: bufSize / (blkSize + 1),
		// Note: make will do initialization
		Data: make([]byte, bufSize),
	}
	buf.NumFreeBlk = buf.NumAllBlk
	return buf
}

// Apply for a new block from a buffer.
// If no free blocks are available in the buffer,
// then the return value is nil and error message;
// otherwise the pointer to the block.
func (buf *Buffer) GetNewBlockBuffer() (blkPtr []byte,err error) {
	if buf.NumFreeBlk == 0 {
		return nil,errors.New("Buffer is full!\n")
	}
	var tmp uint64 = 0
	for tmp < uint64(len(buf.Data)) {
		// AVAILABLE
		if buf.Data[tmp] == 0 {
			break
		} else {
			tmp += buf.BlkSize + 1
		}
	}
	// NOTE:blkptr[0] is not the data
	buf.Data[tmp] = 1
	blkPtr = buf.Data[tmp:]
	buf.NumFreeBlk--
	return blkPtr,nil
}

// Set a block in a buffer to be available.
func (buf *Buffer) FreeBlockInBuffer(blk []byte) {
	blk[0] = 0
	buf.NumFreeBlk++
}

// Drop a block on the disk
func DropBlockOnDisk(addr uint) error {
	fileName := fmt.Sprintf("%d.blk",addr)
	return os.Remove(fileName)
}

// Read a block from the hard disk to the buffer
// by the address of the block.
func (buf *Buffer)ReadBlockFromDisk(addr uint) (blkPtr []byte,err error) {
	if buf.NumFreeBlk == 0 {
		return nil,errors.New("Buffer Overflows!\n")
	}
	var tmp uint64 = 0
	for tmp < uint64(len(buf.Data)) {
		// AVAILABLE
		if buf.Data[tmp] == 0 {
			break
		} else {
			tmp += buf.BlkSize + 1
		}
	}

	fileName := fmt.Sprintf("%d.blk",addr)
	fp, err := os.OpenFile(fileName,os.O_RDONLY,0666)
	defer fp.Close()

	if err != nil {
		return nil,err
	}
	buf.Data[tmp] = 1
	blkPtr = buf.Data[tmp:]
	fp.Read(blkPtr[1:])
	buf.NumFreeBlk--
	buf.NumIO++
	return blkPtr,nil
}

// Read a block in the buffer to the hard disk by the address of the block
func (buf *Buffer)WriteBlockToDisk(blkPtr []byte, addr uint) error {
	fileName := fmt.Sprintf("%d.blk",addr)
	fp, err := os.OpenFile(fileName,os.O_RDWR|os.O_CREATE,0666)
	defer fp.Close()

	if err != nil {
		panic(err)
		return err
	}
	fp.Write(blkPtr[1:buf.BlkSize+1])
	blkPtr[0] = 0
	buf.NumFreeBlk++
	buf.NumIO--
	return  nil
}