package lib

import (
    "fmt"
    _ "io/ioutil"
    _ "os"
    _ "regexp"
    "sort"
    _ "strconv"
    _ "strings"
)

func ProcessSignature() {
    homeDir    := "/Users/sanjaynoronha/go/src/pam"
    anomalyMap := GetAnomalyMap(2) // seed data mapping anomalies types
    feederMap  := GetFeederMap(homeDir + "/data/feeder_metadata.csv")
    datasetMap := GetDatasetMap(homeDir + "/data/pam_1_0_dataset.csv")
    fmt.Printf("Started tickets ...\n")
    ticketMap  := GetTicketMap(homeDir + "/data/tickets")
    fmt.Printf("Finished tickets ...\n")
    _ = anomalyMap
    anomalies := GetAnomalies(homeDir + "/data/all_anoms.csv")
    TruncateAnomalyTimes(anomalies)
    var sortedFeederIds []string
    for feederId, _ := range anomalies {
        sortedFeederIds = append(sortedFeederIds, feederId)
    }
    sort.Strings(sortedFeederIds)
    var y []YObject   = make([]YObject, 0)
    for _, feederId := range sortedFeederIds {
        if len(ticketMap[feederId]) > 0 { // && feederId == "808931" {
            y, _ = transformIntoSignatures(y, anomalies[feederId], ticketMap[feederId], datasetMap, feederMap[feederId], feederId)
        }
    }
    fmt.Printf("Length of y: %d\n", len(y))
}

// anomalies:  map[FeederId]:    [Anomaly1, Anomaly2, ... Anomalyn]
// ticketMap:  map[FeederId]:    [Ticket1, Ticket2, ... Ticketn]
// datasetMap: map[AnomalyType]: [DatasetObject1, DatasetObject2, ... DatasetObjectn]
// feederMap:  map[FeederId]:    [Feeder1, Feeder2, ... Feedern]
func transformIntoSignatures(y []YObject, fAnomalies []Anomaly, fTickets []Ticket, datasetMap map[string]DatasetObject,
    feeder Feeder, feederId string) ([]YObject, []YObject) {
    // fmt.Printf("%s: anomalies:\t%d\ttickets: %d\n", feederId, len(fAnomalies), len(fTickets))

    // Get unique trigger times
    var timeMap map[int64]bool = make(map[int64]bool, 0)
    var times int64arr = make([]int64, 0)
    for _, anom := range fAnomalies {
        timeMap[anom.EpochTime] = true
    }
    for ts, _ := range timeMap {
        times  = append(times, ts)
    }
    sort.Sort(times)

    for _, t := range times {
        yObj := new(YObject)
        yObj.Feeder    = feederId
        yObj.Timestamp = t
        y     = append(y, *yObj)
    }

    for _, ticket := range fTickets {
        // fmt.Printf("TICKET: %s %d\n", ticket.PowerOff, ticket.PowerOffEpoch)
        count := 0
        for _, anomaly := range fAnomalies {
            diffTime := ticket.PowerOffEpoch - anomaly.EpochTime
            if diffTime > 0 && diffTime < 15 * 24 * 3600 {
                // fmt.Printf("\tANOMALY: %s %s %d\n", anomaly.Anomaly, anomaly.Time, anomaly.EpochTime)
                count++
            }
        }
    }
    return y, y
}
