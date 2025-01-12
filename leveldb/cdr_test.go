// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	MAX_CDRs int = 10000
	CDR_BATCHES int = 10
	RES_FILE = "cdrs-results.log"
)


func TestCdrBasic(t *testing.T) {

	if f, err := os.Create(RES_FILE); err == nil {

		results := make(map[int]int)
		dummyValue := []byte{0}
		if db, err := OpenFile("./cdrs", nil); err != nil {
			t.Fatalf("OpenFile %v", err)
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
				t.Logf("Batch %v %v usec", batchID, perCdr)
			}
			getProperties(db, t)
			t.Logf("Done written %d, duplicates %d, put errors %d", written, duplicates, putErrors)

			db.Close()
		}

		for bb:=0; bb<CDR_BATCHES; bb++ {
			fmt.Fprintf(f, "%v,%v\n", bb, results[bb])
		}
		f.Close()
		t.Logf("Done results in %v", RES_FILE)
	} else {
		t.Logf("Error opening results file %v", err)
	}
}


func getProperties(db *DB, t *testing.T) {

	res, err := db.GetProperty("leveldb.stats")
	if err != nil {
		t.Error("leveldb.stats", err)
	} else {
		t.Logf("leveldb.stats %v", res)
	}

	res, err = db.GetProperty("leveldb.compcount")
	if err != nil {
		t.Error("got unexpected error", err)
	} else {
		t.Logf("leveldb.compcount %v", res)
	}
	t.Logf("DB seq=%v", db.seq)
	// _, err = db.GetProperty("leveldb.num-files-at-level0x")
	// if err != nil {
	// 	b.Error("leveldb.num-files-at-level0x", err)
	// } else {
	// 	b.Logf("leveldb.num-files-at-level0x %v", res)
	// }
}
