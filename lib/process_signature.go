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
    _, _, _ = feederMap, anomalyMap, datasetMap
    // fmt.Printf("Num = %d\n", len(feederMap))
    fmt.Printf("%v\n", anomalyMap)
    fmt.Printf("%v\n", datasetMap)
}

func processSignatureFiles(fileName string, fileTag string, fileNum int, writer *bufio.Writer, startTime time.Time, customerMap map[string]int64) {
    
}
