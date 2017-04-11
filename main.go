package main

import (
    "os"
    "strconv"
    "anomaly/lib"
)

func main() {
    argsWithProg := os.Args
    if len(argsWithProg) >= 3 {
        var startFileNumber, endFileNumber int
        _, _ = startFileNumber, endFileNumber
        var err error
        if startFileNumber, err = strconv.Atoi(argsWithProg[1]); err != nil {
            panic(err)
        }
        if endFileNumber, err = strconv.Atoi(argsWithProg[2]); err != nil {
            panic(err)
        }
        // lib.ProcessSCADA(startFileNumber, endFileNumber)
        // lib.ProcessEDNA(startFileNumber, endFileNumber)
    } else {
        // lib.ProcessSCADA(0, -1)
        // lib.ProcessEDNA(0, -1)
    }

    lib.CompareAllAnomsWithEDNAAnoms()
    
}

