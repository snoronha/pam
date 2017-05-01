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
    homeDir        := "/Users/sanjaynoronha/go/src/pam"
    anomalyMap := GetAnomalyMap(2) // seed data mapping anomalies types
    feederMap  := GetFeederMap(homeDir + "/data/feeder_metadata.csv")
    datasetMap := GetDatasetMap(homeDir + "/data/pam_1_0_dataset.csv")
    fmt.Printf("Started tickets ...\n")
    ticketMap  := GetTicketMap(homeDir + "/data/tickets")
    fmt.Printf("Finished tickets ...\n")
    _, _, _ = feederMap, anomalyMap, datasetMap // , ticketMap
    // _ = ticketMap
    // fmt.Printf("%v\n", anomalyMap)
    // for k, arr := range ticketMap {
    // fmt.Printf("feeder %s has %d tickets\n", k, len(arr))
    // }
    anomalies := GetAnomalies(homeDir + "/data/all_anoms.csv")
    var sortedFeederIds []string
    for feederId, _ := range anomalies {
        sortedFeederIds = append(sortedFeederIds, feederId)
    }
    sort.Strings(sortedFeederIds)
    ticketsWithAnoms := 0
    for _, feederId := range sortedFeederIds {
        if len(ticketMap[feederId]) > 0 { // && feederId == "808931" {
            feederTicketsWithAnoms := transformIntoSignatures(anomalies[feederId], ticketMap[feederId], datasetMap, feederMap[feederId], feederId)
            ticketsWithAnoms += feederTicketsWithAnoms
        }
    }
    fmt.Printf("Tickets with anoms: %d\n", ticketsWithAnoms)
}

// anomalies:  map[FeederId]:    [Anomaly1, Anomaly2, ... Anomalyn]
// ticketMap:  map[FeederId]:    [Ticket1, Ticket2, ... Ticketn]
// datasetMap: map[AnomalyType]: [DatasetObject1, DatasetObject2, ... DatasetObjectn]
// feederMap:  map[FeederId]:    [Feeder1, Feeder2, ... Feedern]
func transformIntoSignatures(fAnomalies []Anomaly, fTickets []Ticket, datasetMap map[string]DatasetObject, feeder Feeder, feederId string) int {
    // fmt.Printf("%s: anomalies:\t%d\ttickets: %d\n", feederId, len(fAnomalies), len(fTickets))
    feederTicketsWithAnoms := 0
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
        if count > 0 {
            feederTicketsWithAnoms++
        }
    }
    return feederTicketsWithAnoms
}
