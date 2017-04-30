package lib

import (
    "bufio"
    "fmt"
    _ "io/ioutil"
    _ "os"
    _ "regexp"
    _ "sort"
    _ "strconv"
    _ "strings"
    "time"
)

func ProcessSignature() {
    homeDir        := "/Users/sanjaynoronha/go/src/pam"
    anomalyMap := GetAnomalyMap(2) // seed data mapping anomalies types
    feederMap  := GetFeederMap(homeDir + "/data/feeder_metadata.csv")
    datasetMap := GetDatasetMap(homeDir + "/data/pam_1_0_dataset.csv")
    fmt.Printf("Started tickets ...\n")
    // ticketMap  := GetTicketMap(homeDir + "/data/tickets")
    fmt.Printf("Finished tickets ...\n")
    _, _, _ = feederMap, anomalyMap, datasetMap // , ticketMap
    // _ = ticketMap
    // fmt.Printf("%v\n", anomalyMap)
    // for k, arr := range ticketMap {
    // fmt.Printf("feeder %s has %d tickets\n", k, len(arr))
    // }
    allAnomalies := GetAnomalies(homeDir + "/data/all_anoms.csv")
    _ = allAnomalies
            
}

func processSignatureFiles(fileName string, fileTag string, fileNum int, writer *bufio.Writer, startTime time.Time, customerMap map[string]int64) {
    
}
