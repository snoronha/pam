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
	
    if len(argsWithProg) >= 5 {
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
        _ = monthlyOrBulk
        var isBulkRegexp = regexp.MustCompile(`^b.*`)
        isBulk := isBulkRegexp.MatchString(argsWithProg[3])
        if isBulk {
            monthlyOrBulk = "bulk"
        } else {
            monthlyOrBulk = "monthly"
        }

		awsOrLocal := ""
        _ = awsOrLocal
        var isAWSRegexp = regexp.MustCompile(`^a.*`)
        isAWS := isAWSRegexp.MatchString(argsWithProg[4])
        if isAWS {
            awsOrLocal = "aws"
        } else {
            awsOrLocal = "local"
        }
        // lib.ProcessSCADA(startFileNumber, endFileNumber)
        lib.ProcessEDNA(startFileNumber, endFileNumber, monthlyOrBulk, awsOrLocal)
        // lib.ProcessAMI(startFileNumber, endFileNumber, monthlyOrBulk, awsOrLocal)
    } else {
        fmt.Println("Usage:   anomaly <startFileNumber> <endFileNumber> <monthlyOrBulk> <awsOrLocal>\nExample: anomaly 0 -1 monthly aws")
    }

    // lib.CompareAllAnomsWithEDNAAnoms()
    // lib.SortMergeAnomalyFile("/Users/sanjaynoronha/Desktop/edna_bulk_042617", ".csv", "/Users/sanjaynoronha/Desktop/all_anoms_pf_thd_feb2015", ".csv")
    
}

