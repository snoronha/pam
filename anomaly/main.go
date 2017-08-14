package main

import (
    "flag"
    "fmt"
    "pam/lib"
)


func main() {
    startFileNumberPtr := flag.Int("start", 0, "startFileNumber, an integer")
    endFileNumberPtr   := flag.Int("end", -1, "endFileNumber, an integer")
    isBulkPtr          := flag.Bool("bulk", true, "a boolean for bulk (true) or monthly (false)")
    isLocalPtr         := flag.Bool("local", true, "a boolean for local (true) or AWS (false)")
    flag.Parse()

    fmt.Printf("start=%d end=%d bulk=%v local=%v\n", *startFileNumberPtr, *endFileNumberPtr, *isBulkPtr, *isLocalPtr)
    // lib.ProcessSCADA(*startFileNumberPtr, *endFileNumberPtr)
    lib.ProcessEDNA(*startFileNumberPtr, *endFileNumberPtr, *isBulkPtr, *isLocalPtr)
    // lib.ProcessAMI(*startFileNumberPtr, *endFileNumberPtr, *isBulkPtr, *isLocalPtr)


    // lib.CompareAllAnomsWithEDNAAnoms()
    // lib.SortMergeAnomalyFile("/Users/sanjaynoronha/Desktop/edna_bulk_042617", ".csv", "/Users/sanjaynoronha/Desktop/all_anoms_pf_thd_feb2015", ".csv")
    
}

