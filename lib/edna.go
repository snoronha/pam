package lib

import (
    "strings"
    "time"
)

type IndexedEDNA struct {
    // Extended Id,Time,Value,ValueString,Status
    EdnaLine     string
    Time         time.Time
    EpochTime    int64
}

func (i *IndexedEDNA) Create(ednaLine string) {
    longForm    := "1/2/2006 3:04:05 PM"
    i.EdnaLine   = ednaLine
    lineComponents := strings.Split(ednaLine, ",")
    i.Time, _    = time.Parse(longForm, strings.Replace(lineComponents[1], "\"", "", -1))
    i.EpochTime  = i.Time.Unix()
}
