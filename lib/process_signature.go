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
    anomalyMap := GetAnomalyMap(2)
    feederMap  := GetFeederMap("../data/feeder_metadata.csv")
    datasetMap := GetDatasetMap("../data/pam_1_0_dataset.csv")
    ticketMap  := GetTicketMap("../data/tickets")
    _, _, _, _ = feederMap, anomalyMap, datasetMap, ticketMap
    fmt.Printf("%v\n", anomalyMap)
    for k, arr := range ticketMap {
        fmt.Printf("feeder %s has %d tickets\n", k, len(arr))
    }
}

func processSignatureFiles(fileName string, fileTag string, fileNum int, writer *bufio.Writer, startTime time.Time, customerMap map[string]int64) {
    
}
