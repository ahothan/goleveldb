// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	// "github.com/syndtr/goleveldb"
	"github.com/ahothan/leveldb"
)

var (
	MAX_CDRs int = 10000
	CDR_BATCHES int = 10
	RES_FILE = "cdrs-results.log"
)


func main() {

	if f, err := os.Create(RES_FILE); err == nil {

		results := make(map[int]int)
		dummyValue := []byte{0}
		if db, err := leveldb.OpenFile("./cdrs", nil); err != nil {
			fmt.Printf("OpenFile %v", err)
		} else {
			duplicates := 0
			putErrors := 0
			written := 0
			for batchID := 0; batchID < CDR_BATCHES; batchID++ {
				start := time.Now()
				for i := 0; i < MAX_CDRs; i++ {
					key, _ := uuid.New().MarshalBinary()
					if present, err := db.Has(key, nil); err == nil && present {
						duplicates++
					} else {
						if err := db.Put(key, dummyValue, nil); err != nil {
							putErrors++
						} else {
							written++
						}
					}
				}
				elapsed := time.Since(start)
				perCdr := int(elapsed.Microseconds()/int64(MAX_CDRs))
				results[batchID] = perCdr
				fmt.Printf("Batch %v %v usec", batchID, perCdr)
			}
			res, err := db.GetProperty("leveldb.stats")
			if err != nil {
				fmt.Printf("Error leveldb.stats %v", err)
			} else {
				fmt.Printf("leveldb.stats %v", res)
			}

			res, err = db.GetProperty("leveldb.compcount")
			if err != nil {
				fmt.Printf("Error got unexpected error %v", err)
			} else {
				fmt.Printf("leveldb.compcount %v", res)
			}
			fmt.Printf("DB seq=%v", db.seq)
			fmt.Printf("Done written %d, duplicates %d, put errors %d", written, duplicates, putErrors)

			db.Close()
		}

		for bb:=0; bb<CDR_BATCHES; bb++ {
			fmt.Fprintf(f, "%v,%v\n", bb, results[bb])
		}
		f.Close()
		fmt.Printf("Done results in %v", RES_FILE)
	} else {
		fmt.Printf("Error opening results file %v", err)
	}
}


