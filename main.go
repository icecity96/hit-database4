package main

import (
	"hit-database4/extmem"
	"math/rand"
	"log"
	"sort"
	"hit-database4/btree"
	"github.com/emirpasic/gods/utils"
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

	log.Println("BUILD B-TREE")
	tupleR := loadData(buf,16,0,112)
	tupleS := loadData(buf,32,20,224)

	sort.Slice(tupleR, func(i, j int) bool {
		return tupleR[i].c < tupleR[j].c
	})
	sort.Slice(tupleS, func(i, j int) bool {
		return tupleS[i].c < tupleS[j].c
	})
	btreeR := btree.NewWith(3,utils.ByteComparator)
	for _, tup := range tupleR {
		btreeR.Put(tup.c,tup.blk)
	}
	btreeS := btree.NewWith(3,utils.ByteComparator)
	for _, tup := range tupleS {
		btreeS.Put(tup.c,tup.blk)
	}
	log.Println("B-Tree Finish")

	log.Println("TEST B-TREE")
	resR, _ := btreeR.Get(byte(40))
	log.Println("len(resR) is",len(resR))
	resS, _ := btreeS.Get(byte(60))
	log.Println("len(resS) is",len(resS))
	log.Println("FINISHED TSET B-TREE")

	log.Println("BEGIN TEST INDEXSEARCH")
	indexSearch(buf,resR,resS)
	log.Println("END OF INDEXSEARCH")

	log.Println("BEGIN MappingRelationship")
	mappingRelationship(buf)
	log.Println("END MappingRelationship")

	log.Println("BEGIN Nest-Loop-Join")
	nestLoopJoin(buf)
	log.Println("END OF Nest-Loop-Join")

	log.Println("BEGIN Sort_Merge_Join")
	sortMergeJoin(buf)
	log.Println("END OD Sort_Merge_Join")

	log.Println("BEGIN Hash_Join")
	hashJoin(buf)
	log.Println("END OF Hash_Join")

	log.Println("BEGIN UNION")
	unionOfRelation(buf)
	log.Println("END OF UNION")

	log.Println("BEGIN Intersection")
	intersectionOfRelation(buf)
	log.Println("END OF Intersection")

	log.Println("BEGIN DIFF")
	differenceOfRelation(buf)
	log.Println("END DIFF")
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

// TODO: refactor
func indexSearch(buf *extmem.Buffer,blkR,blkS map[interface{}]bool) int {
	var lineCount uint	= 80
	var numCount 	= 0
	var result		= 0
	blkNew,_ := buf.GetNewBlockBuffer()
	for b,_ := range blkR {
		addr := b.(uint)
		blk,err := buf.ReadBlockFromDisk(addr)
		if err != nil {
			panic(err)
		}
		for j := 0; j < 7; j++ {
			if blk[1+2*j] == 40 {
				log.Printf("(%d, %d)\n", blk[1+2*j], blk[2+2*j])
				blkNew[1+2*numCount] = blk[1+2*j]
				blkNew[2+2*numCount] = blk[2+2*j]
				numCount++
				result++
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
		}
		buf.FreeBlockInBuffer(blk)
	}
	for b,_ := range blkS {
		addr := b.(uint)
		blk,err := buf.ReadBlockFromDisk(addr)
		if err != nil {
			panic(err)
		}
		for j := 0; j < 7; j++ {
			if blk[1+2*j] == 60 {
				log.Printf("(%d, %d)\n", blk[1+2*j], blk[2+2*j])
				blkNew[1+2*numCount] = blk[1+2*j]
				blkNew[2+2*numCount] = blk[2+2*j]
				numCount++
				result++
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
		}
		buf.FreeBlockInBuffer(blk)
	}
	if numCount != 0 {
		buf.WriteBlockToDisk(blkNew, lineCount)
	}
	return result
}

// 实现投影关系
func mappingRelationship(buf *extmem.Buffer) {
	var basicAddr uint 	= 100	// 结果从100.blk 存储
	var numBlk	uint 	= 0
	log.Println("BEFORE SORTED")
	for ri := 0; ri < 16; ri++ {
		blk, err := buf.ReadBlockFromDisk(uint(ri))
		if err != nil {
			panic(err)
		}
		for rj := 0; rj < 7; rj++ {
			blkNew,err := buf.GetNewBlockBuffer()
			if err != nil {
				panic(err)
			}
			log.Println(blk[1+2*rj])
			blkNew[1] = blk[1+2*rj]
			buf.WriteBlockToDisk(blkNew,basicAddr+numBlk)
			numBlk++
		}
		buf.FreeBlockInBuffer(blk)
	}
	log.Println("AFTER SORTED")
	basicAddr = exsort(basicAddr,numBlk,buf,500)
	for ri := 0; uint(ri) < numBlk; ri++ {
		blk,err := buf.ReadBlockFromDisk(basicAddr+uint(ri))
		if err != nil {
			panic(err)
		}
		log.Println(blk[1])
		buf.FreeBlockInBuffer(blk)
	}
	log.Println("After distinct")
	blk,err := buf.ReadBlockFromDisk(basicAddr)
	blkNew,err := buf.GetNewBlockBuffer()
	if err != nil {
		panic(err)
	}
	count := 0
	countA := 0
	var tmp = blk[1]
	blkNew[1] = blk[1]
	log.Println(tmp)
	for ri := 0; uint(ri) < numBlk; ri++ {
		for blk[1] == tmp {
			buf.FreeBlockInBuffer(blk)
			ri++
			if uint(ri) < numBlk {
				blk,err = buf.ReadBlockFromDisk(basicAddr+uint(ri))
				if err != nil {
					panic(err)
				}
			} else {
				goto END
			}
		}
		tmp = blk[1]
		count++
		blkNew[1+count] = tmp
		if uint(ri) == numBlk-1 {
			buf.WriteBlockToDisk(blkNew,basicAddr+uint(150+countA))	// 250开始
		} else {
			if count == 15 {
				blkNew[count+2] = byte(basicAddr+uint(150+countA+1))
				buf.WriteBlockToDisk(blkNew,basicAddr+uint(150+countA))
				count = -1
				countA++
			}
		}
		log.Println(tmp)
		buf.FreeBlockInBuffer(blk)
	}
	END:
}

// 归并排序
func exsort(sAddr,numBlk uint,buf *extmem.Buffer,addr uint) uint {
	if numBlk == 1 {
		return sAddr
	}
	left := exsort(sAddr,numBlk/2,buf,addr)
	right := exsort(sAddr+numBlk/2,numBlk-numBlk/2,buf,addr)
	return merge(left,numBlk/2,right,numBlk-numBlk/2,buf,addr)
}

func merge(left, ln, right, rn uint, buf *extmem.Buffer, addr uint) uint {
	var l uint = 0
	var r uint = 0
	var tempAddr uint = left+addr
	var offset uint = 0
	var blk1 []byte
	var blk2 []byte
	var err error
	for l < ln || r < rn {
		if l != ln {
			blk1,err = buf.ReadBlockFromDisk(left+l)
			if err != nil {
				panic(err)
			}
		}

		if r != rn {
			blk2,err = buf.ReadBlockFromDisk(right+r)
			if err != nil {
				panic(err)
			}
		}

		if r == rn || blk1[1] < blk2[1] {
			buf.WriteBlockToDisk(blk1,tempAddr+offset)
			buf.FreeBlockInBuffer(blk2)
			l++
			offset++
		} else {
			buf.WriteBlockToDisk(blk2,tempAddr+offset)
			buf.FreeBlockInBuffer(blk1)
			r++
			offset++
		}
	}
	// write back
	for i := 0; uint(i) < offset; i++ {
		blk1,_ = buf.ReadBlockFromDisk(tempAddr+uint(i))
		buf.WriteBlockToDisk(blk1,left+uint(i))
	}
	return left
}

// Nest Loop Join就是通过两层循环手段进行依次的匹配操作，最后返回结果集合。
func nestLoopJoin(buf *extmem.Buffer) uint {
	var basicAddr uint = 800
	var blkCount uint  = 0
	var count uint = 0
	var blkNew []byte
	for ri := 0; ri < 16; ri++ {	// each block of R
		blkR,err := buf.ReadBlockFromDisk(uint(ri))
		if err != nil {
			panic(err)
		}
		for rj := 0; rj < 7; rj++ {
			for si := 0; si < 32; si++ {	// each block of S
				blkS,err := buf.ReadBlockFromDisk(uint(si+20))
				if err != nil {
					panic(err)
				}
				for sj := 0; sj < 7; sj++ {
					if blkR[1+2*rj] == blkS[1+2*sj] {
						if blkCount == 0 {
							blkNew,err = buf.GetNewBlockBuffer()
							if err != nil {
								panic(err)
							}
						}
						blkNew[1+blkCount] = blkR[1+2*rj]
						blkNew[2+blkCount] = blkR[2+2*rj]
						blkNew[3+blkCount] = blkR[2+2*sj]
						log.Printf("(%d, %d, %d)",blkNew[1+blkCount],blkNew[2+blkCount],blkNew[3+blkCount])
						count++
						blkCount += 3
						if blkCount == 15 {
							blkNew[1+blkCount] = byte(basicAddr+1)
							buf.WriteBlockToDisk(blkNew,basicAddr)
							basicAddr++
							blkCount = 0
						}
					}
				}
				if blkS[0] == 1 {
					buf.FreeBlockInBuffer(blkS)
				}
			}
		}
		if blkR[0] == 1 {
			buf.FreeBlockInBuffer(blkR)
		}
		if blkCount != 0 {
			buf.WriteBlockToDisk(blkNew,basicAddr)
		}
	}
	return count
}

// 排序合并连接 (Sort Merge Join)是一种两个表在做连接时用排序操作(Sort)和合并操作(Merge)来得到连接结果集的连接方法。
// 对于排序合并连接的优缺点及适用场景如下：
// a,通常情况下，排序合并连接的执行效率远不如哈希连接，但前者的使用范围更广，因为哈希连接只能用于等值连接条件，
// 而排序合并连接还能用于其他连接条件(如<,<=,>.>=)
// b,通常情况下，排序合并连接并不适合OLTP类型的系统，其本质原因是对于因为OLTP类型系统而言，
// 排序是非常昂贵的操作，当然，如果能避免排序操作就例外了。
func sortMergeJoin(buf *extmem.Buffer) uint {
	var basicAddr uint = 900
	var count uint = 0
	var wCount uint = 0
	var flag int = 0
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
	var i int = 0
	var j int = 0
	blkNew,_ := buf.GetNewBlockBuffer()
	for i < 112 && j < 224 {
		if tupleR[i].c == tupleS[j].c {
			log.Printf("(%d, %d, %d)",tupleR[i].c,tupleR[i].d,tupleS[j].d)
			blkNew[1+wCount] = tupleR[i].c
			blkNew[2+wCount] = tupleR[i].d
			blkNew[3+wCount] = tupleS[i].d
			wCount += 3
			if i == 0 || tupleR[i-1].c < tupleR[i].c {
				flag = i
			}
			count++
			if i == 111 {
				i = flag
				j++
			} else {
				i++
			}
		} else if tupleR[i].c < tupleS[j].c {
			i++
		} else {
			j++
			if tupleS[j].c == tupleS[j-1].c {
				i = flag
			}
		}
		if wCount == 15 {
			blkNew[1+wCount] = byte(basicAddr+1)
			buf.WriteBlockToDisk(blkNew,basicAddr)
			basicAddr++
			wCount = 0
		}
	}
	if wCount != 0 {
		buf.WriteBlockToDisk(blkNew,basicAddr)
	}
	return count
}

// 散列连接是CBO 做大数据集连接时的常用方式，优化器使用两个表中较小的表（或数据源）利用连接键在内存中建立散列表，
// 然后扫描较大的表并探测散列表，找出与散列表匹配的行。
// 在本次实现中，两个表都比较小，所以直接都在内存中建立散列表了
func hashJoin(buf *extmem.Buffer) uint {
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
	// 桶R S
	bucketR, bucketRInt := fillBucket(buf,0,16)
	bucketS, bucketSInt := fillBucket(buf,20,32)
	blkNew,_ := buf.GetNewBlockBuffer()
	var basicAddr uint = 1000
	var wCount int = 0
	var count uint = 0
	for i := 0; i < 5; i++ {
		numR := bucketRInt[i]
		numS := bucketSInt[i]
		for rj := 0; rj < numR; rj++ {
			for sj := 0; sj < numS; sj++ {
				if bucketR[i][rj].c == bucketS[i][sj].c {
					log.Printf("(%d, %d, %d)",bucketR[i][rj].c,bucketR[i][rj].d,bucketS[i][sj].d)
					blkNew[1+wCount] = tupleR[i].c
					blkNew[2+wCount] = tupleR[i].d
					blkNew[3+wCount] = tupleS[i].d
					wCount += 3
					count++
				}
				if wCount == 15 {
					blkNew[1+wCount] = byte(basicAddr+1)
					buf.WriteBlockToDisk(blkNew,basicAddr)
					basicAddr++
					wCount = 0
				}
			}
		}
	}
	if wCount != 0 {
		buf.WriteBlockToDisk(blkNew,basicAddr)
	}
	return count
}

func fillBucket(buf *extmem.Buffer,addr, len int) ([]([]tuple),[]int) {
	bucket := make([]([]tuple),5)
	for i := 0; i < 5; i++ {
		bucket[i] = make([]tuple,len*7)
	}
	bucketCon := make([]int,5)
	for i := 0; i < len; i++ {
		blk,err := buf.ReadBlockFromDisk(uint(addr+i))
		if err != nil {
			panic(err)
		}
		for j := 0; j < 7; j++ {
			sel := blk[1+2*j] % 5
			p := bucketCon[sel]
			bucket[sel][p].c = blk[1+2*j]
			bucket[sel][p].d = blk[2+2*j]
			bucketCon[sel]++
		}
		buf.FreeBlockInBuffer(blk)
	}
	return bucket,bucketCon
}

func calculationIntersection(buf *extmem.Buffer) []tuple {
	res := make([]tuple,1000)	// make big enough
	var count int = 0
	for ri := 0; ri < 16; ri++ {
		blkR,_ := buf.ReadBlockFromDisk(uint(ri))
		for rj := 0; rj < 7; rj++ {
			for si := 0; si < 32; si++ {
				blkS,_ := buf.ReadBlockFromDisk(uint(si+20))
				for sj := 0; sj < 7; sj++ {
					if blkR[1+2*rj] == blkS[1+2*sj] && blkR[2+2*rj] == blkS[2+2*sj]{
						res[count].c = blkS[1+2*sj]
						res[count].d = blkS[2+2*sj]
						count++
					}
				}
				buf.FreeBlockInBuffer(blkS)
			}
		}
		buf.FreeBlockInBuffer(blkR)
	}
	return res[:count]
}

// 并集操作
func unionOfRelation(buf *extmem.Buffer) {
	temp := calculationIntersection(buf)
	var scount int = 0
	var basicAddr uint = 1500
	var blkNew []byte
	var numDiff uint = 0
	for si := 0; si < 32; si++ {
		blk,_ := buf.ReadBlockFromDisk(uint(si+20))
		blkNew,_ = buf.GetNewBlockBuffer()
		for sj := 0; sj < 7; sj++ {
			inR := false
			// 判断是否在R中
			for i := 0; i < len(temp); i++ {
				if blk[1+2*sj] == temp[i].c && blk[2+2*sj] == temp[i].d {
					inR = true
					break
				}
			}
			// 不在则添加
			if !inR {
				blkNew[1+scount*2] = blk[1+2*sj]
				blkNew[2+scount*2] = blk[2+2*sj]
				scount++
				numDiff++
			}

			if scount == 7 {
				blkNew[16] = byte(basicAddr+1)
				buf.WriteBlockToDisk(blkNew,basicAddr)
				basicAddr++
				scount = 0
			}
		}
		buf.FreeBlockInBuffer(blk)
	}

	if scount != 0 {
		buf.WriteBlockToDisk(blkNew,basicAddr)
		basicAddr++
	}

	if len(temp) == 0 {
		log.Println("NO SAME")
	} else {
		log.Printf("R和S拥有%d项重复项,%d",len(temp),numDiff)
	}
	dataShow(buf,0,16,112)
	dataShow(buf,1500,int(basicAddr),numDiff)
}

// 交集操作
func intersectionOfRelation(buf *extmem.Buffer) {
	temp := calculationIntersection(buf)
	if len(temp) == 0 {
		log.Println("R和S交集是空集")
		return
	}
	var basicAddr uint = 2000
	var tempCount int = 0
	for i := 0; i < len(temp); i++ {
		blkNew,_ := buf.GetNewBlockBuffer()
		var j int
		for j = 0; j < 7 && tempCount < len(temp); j++ {
			blkNew[1+2*j] = temp[tempCount].c
			blkNew[2+2*j] = temp[tempCount].d
			log.Printf("(%d, %d)",temp[tempCount].c,temp[tempCount].d)
			tempCount++
		}
		blkNew[2+2*j] = byte(basicAddr+1)
		buf.WriteBlockToDisk(blkNew,basicAddr)
		basicAddr++
	}
}

// 差集操作
func differenceOfRelation(buf *extmem.Buffer) {
	temp := calculationIntersection(buf)
	var count int = 0
	var basicAddr uint
	var resnum int = 0
	log.Println("BEGIN R-S")
	basicAddr = 2500
	num := 16
	begin := 0

	var blkNew []byte
	for i := begin; i < num; i++ {
		flag := false
		blk,_ := buf.ReadBlockFromDisk(uint(i))
		blkNew,_ = buf.GetNewBlockBuffer()
		for j := 0; j < 7; j++ {
			for k := 0; k < len(temp); k++ {
				if blk[1+2*j] == temp[k].c && blk[2+2*j] == temp[k].d {
					flag = true
					break
				}
				if !flag {
					blkNew[1+2*count] = blk[1+2*j]
					blkNew[2+2*count] = blk[2+2*j]
					log.Printf("(%d, %d)",blkNew[1+2*count],blkNew[2+2*count])
					count++
					resnum++
				}
				if count == 7 {
					buf.WriteBlockToDisk(blkNew,basicAddr)
					blkNew,_ = buf.GetNewBlockBuffer()
					basicAddr++
				}
			}
		}
		buf.FreeBlockInBuffer(blk)
	}
	if count != 0 {
		buf.WriteBlockToDisk(blkNew,basicAddr)
	}
	log.Println("END OF R-S")
	log.Println(resnum)
}