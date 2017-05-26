package main

import (
	"hit-database4/extmem"
	"math/rand"
	"log"
	"sort"
)


func main() {
	// Initialize Buffer
	buf := extmem.NewBuffer(520,64)
	/*
	log.Println("WE BEGIN TO GEN DATA")
	genDataR(buf)
	genDataS(buf)
	log.Println("WE HAVE FINISHED GEN DATA")
	*/
	log.Println("BEGING TO PRINT DATA OF R")
	buf = dataShow(buf,0,16,112)
	log.Println("FINISH PRINT DATA OF R")

	log.Println("BEGIN TO PRINT DATA OF S")
	buf = dataShow(buf,20,52,224)
	log.Println("FINISH PRINT DATA OF S")

	log.Println("BEGIN LINEAR SEARCH")
	res := linearSearch(buf)
	log.Println("FINISH LINEAR SEARCH")

	log.Println("Verify linearSearch")
	dataShow(buf,60,62,res)
	log.Println("END of Verify")

	log.Println("BEGIN Binary Search")
	res = binarySearch(buf)
	log.Println("END OF Binary Search")

	log.Println("Verify binarySearch")
	dataShow(buf,70,72,res)
	log.Println("END of Verify")
}

// 用于展示数据
func dataShow(buf *extmem.Buffer,begin,end int,num uint) *extmem.Buffer {
	var tmp uint = 0
	for i := begin; i < end; i++ {
		blk, err := buf.ReadBlockFromDisk(uint(i))
		if err != nil {
			panic(err)
		}
		for j := 0; tmp < num && j < 7; j++ {
			log.Printf("(%d, %d) ", blk[1+2*j], blk[2+2*j])
			tmp++
		}
		log.Println()
		buf.FreeBlockInBuffer(blk)
	}
	return buf
}

// 产生介于[min,max]之间的随机数
func random(min, max int) int {
	return rand.Int()%(max - min) + min + 1
}

// genDataR 负责生成数据，并存储到R中
func genDataR(buf *extmem.Buffer) {
	for ri := 0; ri < 16; ri++ {
		blk,err := buf.GetNewBlockBuffer()
		if err != nil {
			panic(err)
		}
		for rj := 0; rj < 7; rj++ {
			blk[1+2*rj] = byte(random(1,40))
			blk[2+2*rj] = byte(random(1,1000))
		}

		if ri == 15 {
			blk[16] = byte(0)
		} else {
			blk[16] = byte(ri+1)
		}

		err = buf.WriteBlockToDisk(blk,uint(ri))
		if err != nil {
			panic(err)
		}
	}
}

func genDataS(buf *extmem.Buffer) {
	for si := 0; si < 32; si++ {
		blk, err := buf.GetNewBlockBuffer()
		if err != nil {
			panic(err)
		}
		for sj := 0; sj < 7; sj++ {
			blk[1+2*sj] = byte(random(20,60))
			blk[2+2*sj] = byte(random(1,1000))
		}
		if si == 31 {
			blk[16] = byte(0)
		} else {
			blk[16] = byte(si+21)
		}

		err = buf.WriteBlockToDisk(blk,uint(si+20))
		if err != nil {
			panic(err)
		}
	}
}

// 线性查找
func linearSearch(buf *extmem.Buffer) uint{
	// 结果从60开始存储
	var lineCount uint = 60
	var numberCount uint = 0
	var result uint = 0
	blkNew, err := buf.GetNewBlockBuffer()
	if err != nil {
		panic(err)
	}
	// Search R
	buf, blkNew, numberCount, result, lineCount = linearFind(buf, blkNew, numberCount, result, lineCount,0,16,40)

	// Search S
	buf, blkNew, numberCount, result, lineCount = linearFind(buf, blkNew, numberCount, result, lineCount,20,52,60)

	// 把剩余部分写入磁盘
	if numberCount != 0 {
		buf.WriteBlockToDisk(blkNew, lineCount)
	}
	return result
}

func linearFind(buf *extmem.Buffer, blkNew []byte, numberCount uint, result uint, lineCount uint,begin int,end int,val byte) (*extmem.Buffer, []byte, uint, uint, uint) {
	for i := begin; i < end; i++ {
		blk, err := buf.ReadBlockFromDisk(uint(i))
		if err != nil {
			panic(err)
		}
		for j := 0; j < 7; j++ {
			if blk[1+2*j] == val {
				log.Printf("(%d, %d)\n", blk[1+2*j], blk[2+2*j])
				blkNew[1+2*numberCount] = blk[1+2*j]
				blkNew[2+2*numberCount] = blk[2+2*j]
				numberCount++
				result++
				if numberCount == 7 {
					numberCount = 0
					err = buf.WriteBlockToDisk(blkNew, lineCount)
					blkNew, err = buf.GetNewBlockBuffer()
					if err != nil {
						panic(err)
					}
					lineCount++
				}
			}
		}
		buf.FreeBlockInBuffer(blk)
	}
	return buf, blkNew, numberCount, result, lineCount
}

// 二分搜索
func binarySearch(buf *extmem.Buffer) uint {
	// step1: get data
	tupleR := loadData(buf,16,0,112)
	tupleS := loadData(buf,32,20,224)
	// step2: sort data
	sort.Slice(tupleR, func(i, j int) bool {
		return tupleR[i].c < tupleR[j].c
	})
	sort.Slice(tupleS, func(i, j int) bool {
		return tupleS[i].c < tupleS[j].c
	})
	// step3: binary search
	beginR,endR := binaryIndex(tupleR,40)
	beginS,endS := binaryIndex(tupleS,60)
	// setp4: write it to disk
	blkNew,err := buf.GetNewBlockBuffer()
	if err != nil {
		panic(err)
	}
	var numCount int = 0
	var result uint = uint(endR-beginR+endS-beginS)
	// write disk from 70
	var lineCount uint = 70
	blkNew,numCount,lineCount = storeData(beginR, endR, tupleR, blkNew, numCount, err, buf, lineCount)
	blkNew,numCount,lineCount = storeData(beginS, endS, tupleS, blkNew, numCount, err, buf, lineCount)
	if numCount != 0 {
		buf.WriteBlockToDisk(blkNew, lineCount)
	}
	return result
}

func storeData(begin int, end int, tuples []tuple, blkNew []byte, numCount int, err error, buf *extmem.Buffer, lineCount uint)([]byte,int,uint) {
	if begin == -1 || end == -1 {
		return blkNew, numCount, lineCount
	}
	for i := begin; i < end; i++ {
		log.Printf("(%d, %d)", tuples[i].c, tuples[i].d)
		blkNew[1+2*numCount] = tuples[i].c
		blkNew[2+2*numCount] = tuples[i].d
		numCount++
		if numCount == 7 {
			numCount = 0
			err = buf.WriteBlockToDisk(blkNew, lineCount)
			blkNew, err = buf.GetNewBlockBuffer()
			if err != nil {
				panic(err)
			}
			lineCount++
		}
	}
	return blkNew, numCount, lineCount
}

type tuple struct {
	c byte
	d byte
	blk uint
}

func loadData(buf *extmem.Buffer, len, addr, tuplenum int) []tuple {
	result := make([]tuple, tuplenum)
	var tempnum = 0
	for i := 0; i < len; i++ {
		blk, err := buf.ReadBlockFromDisk(uint(addr+i))
		if err != nil {
			panic(err)
		}
		for j := 0; tempnum < tuplenum && j < 7; j++ {
			result[tempnum].c = blk[1+2*j]
			result[tempnum].d = blk[2+2*j]
			result[tempnum].blk = uint(addr+i)
			tempnum++
		}
		buf.FreeBlockInBuffer(blk)
	}
	return result
}

func binaryIndex(tuples []tuple, val byte) (begin,end int) {
	var left int = 0
	var right int = len(tuples)-1
	for left <= right {
		mid := (left + right)/2
		if tuples[mid].c == val {
			begin,end = mid,mid
			for begin >= left {
				if tuples[begin-1].c == val {
					begin--
				} else {
					break
				}
			}
			for end <= right {
				if tuples[end].c == val {
					end++
				} else {
					break
				}
			}
			return
		} else if tuples[mid].c < val {
			left = mid + 1
		} else {
			right = mid + 1
		}
	}
	return -1,-1
}
