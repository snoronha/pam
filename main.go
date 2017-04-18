package main

import (
    "fmt"
    "os"
    "regexp"
    "strconv"
    "anomaly/lib"
)

func main() {
    argsWithProg := os.Args
    if len(argsWithProg) >= 4 {
        var startFileNumber, endFileNumber int
        _, _ = startFileNumber, endFileNumber
        var err error
        if startFileNumber, err = strconv.Atoi(argsWithProg[1]); err != nil {
            panic(err)
        }
        if endFileNumber, err = strconv.Atoi(argsWithProg[2]); err != nil {
            panic(err)
        }
        monthlyOrBulk := ""
        var isBulkRegexp = regexp.MustCompile(`^b.*`)
        isBulk := isBulkRegexp.MatchString(argsWithProg[3])
        if isBulk {
            monthlyOrBulk = "bulk"
        } else {
            monthlyOrBulk = "monthly"
        }
        // lib.ProcessSCADA(startFileNumber, endFileNumber)
        lib.ProcessEDNA(startFileNumber, endFileNumber, monthlyOrBulk)
    } else {
        fmt.Println("Usage:   anomaly <startFileNumber> <endFileNumber> <monthlyOrBulk>\nExample: anomaly 0 -1 monthly")
    }

    // lib.CompareAllAnomsWithEDNAAnoms()
    
}

