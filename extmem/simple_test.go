package extmem

import (
	"testing"
	"fmt"
)

func TestALL(t *testing.T) {
	buf := NewBuffer(20,8)

	blk, _ := buf.GetNewBlockBuffer()
	for i := 1; i < 9; i++ {
		blk[i] = byte(int('a') + i)
	}
	buf.WriteBlockToDisk(blk,31415926)
	blk, _ = buf.ReadBlockFromDisk(31415926)
	for i := 1; i < 9; i++ {
		fmt.Println(string(blk[i]))
	}
	fmt.Println("IO's is",buf.NumIO)
}
