package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	lvlopt "github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	KiB = 1024
	MiB = KiB * 1024

	cachedKeyNum = 100000000
)

var (
	//keySize   = flag.Int("keysz", 0, "Key size in bytes.")
	valueSize = flag.Int("valsz", 0, "Value size in bytes.")
	bsize     = flag.Int("batchsz", 1, "How many keys each batch write.")
	start     = flag.Int("start", 1, "data write count range start.")
	times     = flag.Int("times", 1, "How many times batch write.")

	value1024 = "5f7575632450456b625c49725d336a7e2158245a7c652b6e2252495e453254353f5b453c5e50" +
		"5d6d734c732c2a40763d4c294168347c4638535a2f7730444c215a5333534d745c353733554f5c3d6d545" +
		"e474d286e4e2849554928433379315d366726477d72454628362a6a3b442c716c6f2a544e63565c4f2827" +
		"443f662527696561405d296f56703727474a2b657d322e316b2d57443971655724555d3c4129783c6a292" +
		"2405555665e5c644f2769217176634c613c4358265634502a546350754c25384e2c786c652f5775623332" +
		"52292a48202e685e5b382d353f7a7c6d22617c692e774d69366a646b696a51294c3162334b65425067327" +
		"d74307c246221523a6a697356393c66345e687e7b763362357851764f552159695f2f7876664e54657c54" +
		"24563844492c664a4021436a6d70222b795670534370502032623b434f3a286f2f35453f2d517a50666d6" +
		"c4c29224e4673655a2c4f2f57637d43756a2e756d7d236e5c4674326d2c2c2b3e51734362246a7d697e2d" +
		"46733d5d337a376746443e6122217225727024205c2f7825687d5a52332328606963293857393b2841396" +
		"b225f73652f533f302e7359522d2a634b2e6f2b236e7a66432c6d6d7851565e385a494146433f3332573d" +
		"5225542c5c29525861703c2956215e4e24514e6b32233e2a3b3b5d406e2c6b5525426135683d563d6a5e7335786e757e47"

	value1536 = "5f7575632450456b625c49725d336a7e2158245a7c652b6e2252495e453254353f5b453c5e50" +
		"5d6d734c732c2a40763d4c294168347c4638535a2f7730444c215a5333534d745c353733554f5c3d6d545" +
		"e474d286e4e2849554928433379315d366726477d72454628362a6a3b442c716c6f2a544e63565c4f2827" +
		"443f662527696561405d296f56703727474a2b657d322e316b2d57443971655724555d3c4129783c6a292" +
		"2405555665e5c644f2769217176634c613c4358265634502a546350754c25384e2c786c652f5775623332" +
		"52292a48202e685e5b382d353f7a7c6d22617c692e774d69366a646b696a51294c3162334b65425067327" +
		"d74307c246221523a6a697356393c66345e687e7b763362357851764f552159695f2f7876664e54657c54" +
		"24563844492c664a4021436a6d70222b795670534370502032623b434f3a286f2f35453f2d517a50666d6" +
		"c4c29224e4673655a2c4f2f57637d43756a2e756d7d236e5c4674326d2c2c2b3e51734362246a7d697e2d" +
		"46733d5d337a376746443e6122217225727024205c2f7825687d5a52332328606963293857393b2841396" +
		"b225f73652f533f302e7359522d2a634b2e6f2b236e7a66432c6d6d7851565e385a494146433f3332573d" +
		"5225542c5c29525861703c2956215e4e24514e6b32233e2a3b3b5d406e2c6b5525426135683d563d6a5e7335786e757e47" +
		"5225542c5c29525861703c2956215e4e24514e6b32233e2a3b3b5d406e2c6b5525426135683d563d6a5e73" +
		"35786e757e475225542c5c29525861703c2956215e4e24514e6b32233e2a3b3b5d406e2c6b552542613568" +
		"3d563d6a5e7335786e757e475225542c5c29525861703c2956215e4e24514e6b32233e2a3b3b5d406e2c6b" +
		"5525426135683d563d6a5e7335786e757e475225542c5c29525861703c2956215e4e24514e6b32233e2a3b" +
		"3b5d406e2c6b5525426135683d563d6a5e7335786e757e475225542c5c29525861703c2956215e4e24514e6" +
		"b32233e2a3b3b5d406e2c6b5525426135683d563d6a5e7335786e757e4763d6a5e7335786e757e472"

	value10 = "5f75756324"

	value16KB = ""
)

var cachedKey []*keyMeta

func init() {
	for i := 0; i < 16; i++ {
		value16KB = value16KB + value1024
	}

	cachedKey = make([]*keyMeta, 0, cachedKeyNum) // cache 100 million least input key
}

type keyMeta struct {
	indexI int
	indexJ int
	rInt   int
}

type entry struct {
	Key   []byte
	Value []byte
	Meta  byte
}

func zxlKey(valueSz, rint, indexi, indexj int) string {
	return fmt.Sprintf("nft_vsz=%010d-k=%036d-%05d-%05d_39", rint, valueSz, indexj, indexi) // 72 bytes.
}

func fillEntryWithIndex(e *entry, valueSz int, key string) {
	//tmp := "nft_e5151b3cecdf2253182132a2d88ef094e9e3e745387bf0cf3b366ff65b1d5507_393"
	//key := fmt.Sprintf("vsz=%036d-k=%010d-%010d", *valueSize, k, index) // 64 bytes.
	//key := fmt.Sprintf("nft_vsz=%036d-k=%010d-%05d-%05d_39", valueSz, rand.Intn(10000000), indexj, indexi) // 64 bytes.
	//key := zxlKey(valueSz, rand.Intn(100000000), indexi, indexj)
	if cap(e.Key) < len(key) {
		e.Key = make([]byte, 2*len(key))
	}
	e.Key = e.Key[:len(key)]
	copy(e.Key, key)

	if valueSz == 10 {
		e.Value = []byte(value10)
	} else if valueSz == 1024 {
		e.Value = []byte(value1024)
	} else if valueSz == 1536 {
		e.Value = []byte(value1536)
	} else if valueSz == 16384 {
		e.Value = []byte(value16KB)
	} else {
		rCnt := valueSz
		p := make([]byte, rCnt)
		r := rand.New(rand.NewSource(time.Now().Unix()))
		for i := 0; i < rCnt; i++ {
			p[i] = ' ' + byte(r.Intn('~'-' '+1))
		}
		e.Value = p[:valueSz]
	}

	//rCnt := 100
	//p := make([]byte, rCnt)
	//r := rand.New(rand.NewSource(time.Now().Unix()))
	//for i := 0; i < rCnt; i++ {
	//	p[i] = ' ' + byte(r.Intn('~'-' '+1))
	//}
	//
	//valueCap := *valueSize + (rCnt - *valueSize % rCnt)
	//b := make([]byte, 0, valueCap)
	//for len(b) < valueCap {
	//	b = append(b, p...)
	//}
	//e.Value = b[:*valueSize]
	e.Meta = 0
}

var level *leveldb.DB

func main() {
	//valueSz := 1024
	//dataCntRange := 10
	//skip := 1
	//batchSize := 10000

	flag.Parse()
	keySz := 72 //*keySize
	valueSz := *valueSize
	startPoint := *start
	batchSize := *bsize
	wTimes := *times

	if startPoint < 1 {
		startPoint = 1
	}

	fmt.Printf("keySz: %d\n", keySz)
	fmt.Printf("valueSz: %d\n", valueSz)
	fmt.Printf("startPoint: %d\n", startPoint)
	fmt.Printf("batchSize: %d\n", batchSize)

	leveldbTest(keySz, valueSz, batchSize, startPoint, wTimes)
}

func leveldbTest(keysz, valuesz, batchSize, startPoint, wTimes int) {
	var err error
	rand.Seed(time.Now().Unix())
	lPath := fmt.Sprintf("./data/level-k%d-v%d", keysz, valuesz)
	_ = os.MkdirAll(lPath, 0777)
	opt := &lvlopt.Options{}
	opt.CompactionL0Trigger = 16
	opt.WriteL0SlowdownTrigger = 20
	opt.WriteL0PauseTrigger = 24
	opt.NoSync = true
	opt.DisableLargeBatchTransaction = false
	opt.WriteBuffer = 8 * MiB
	opt.CompactionTableSize = 8 * MiB
	opt.CompactionTotalSize = 40 * MiB
	level, err = leveldb.OpenFile(lPath, opt)
	if err != nil {
		panic(err)
	}
	defer func() { _ = level.Close() }()
	infoStr := fmt.Sprintf("keysz: %d, valsz: %d, bacthsz: %d", keysz, valuesz, batchSize)

	for i := startPoint; i < wTimes; i++ {
		lStart := time.Now()
		entries := make([]*entry, 0, batchSize)
		for k := 0; k < batchSize; k++ {
			e := new(entry)
			rint := rand.Intn(cachedKeyNum)
			addCacheKey(i, k, rint)
			key := zxlKey(valuesz, rint, i, k)
			fillEntryWithIndex(e, valuesz, key)
			entries = append(entries, e)
		}

		lb := &leveldb.Batch{}
		for j := 0; j < batchSize; j++ {
			lb.Put(entries[j].Key, entries[j].Value)
		}

		wStart := time.Since(lStart)
		err = level.Write(lb, &lvlopt.WriteOptions{Sync: false})
		if err != nil {
			panic(err)
		}
		wEnd := time.Since(lStart)

		//for ri := 0; ri < batchSize/1000; ri++ {
		wg := sync.WaitGroup{}
		wg.Add(batchSize)
		for ri := 0; ri < batchSize; ri++ {
			go func() {
				defer wg.Done()
				keyM := cachedKey[rand.Intn(len(cachedKey))]
				key := zxlKey(valuesz, keyM.rInt, keyM.indexI, keyM.indexJ)
				_, err = level.Get([]byte(key), nil)
				if err != nil && err != leveldb.ErrNotFound {
					panic(err)
				}
			}()
		}

		wg.Wait()
		rEnd := time.Since(lStart)
		fmt.Println(fmt.Sprintf("leveldb %s write %d st data, time used: prepare: %d, write: %d, read: %d",
			infoStr, i, wStart.Milliseconds(), (wEnd - wStart).Milliseconds(), (rEnd - wEnd).Milliseconds()))
	}
}

func addCacheKey(indexI, indexJ, rInt int) {
	cachedKey = append(cachedKey, &keyMeta{
		indexI: indexI,
		indexJ: indexJ,
		rInt:   rInt,
	})
	if len(cachedKey) >= cachedKeyNum {
		cachedKey = cachedKey[10000:]
	}
}
