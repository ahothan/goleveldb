// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

var MAX_CDRs = 100000

func TestCdrBasic(t *testing.T) {

	dummyValue := []byte{0}
	if db, err := OpenFile("./cdrs", nil); err != nil {
		t.Fatalf("OpenFile %v", err)
	} else {
		duplicates := 0
		putErrors := 0
		written := 0
		start := time.Now()
		for i := 0; i < MAX_CDRs; i++ {
			key, _ := uuid.New().MarshalBinary()
			if _, err := db.Has(key, nil); err != nil {
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
		getProperties(db, t)
		db.Close()
		t.Logf("Done written %d, duplicates %d, put errors %d", written, duplicates, putErrors)
		t.Logf("Elapsed %v, throughout=%v cdr/sec", elapsed, float64(written)*1000/float64(elapsed.Milliseconds()))
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
