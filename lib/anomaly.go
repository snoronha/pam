package lib

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "strings"
    "time"
)

type Anomaly struct {
    Id          string
    Anomaly     string
    DeviceId    string
    DevicePhase string
    DeviceType  string
    FeederId    string
    Signal      string
    Value       string
    Time        string
    EpochTime   int64
}

func (a *Anomaly) Populate(id string, anomaly string, deviceId string, devicePhase string, deviceType string,
    feederId string, signal string, value string, tm time.Time) {
    a.Id          = id
    a.Anomaly     = anomaly
    a.DeviceId    = deviceId
    a.DevicePhase = devicePhase
    a.DeviceType  = deviceType
    a.FeederId    = feederId
    a.Signal      = signal
    a.Value       = value
    a.Time        = tm.String()
    a.EpochTime   = tm.Unix()
}

func (a *Anomaly) Create(anomalyLine string) {
    // e.g. new form 2012-01-01 00:03:07 +0000 UTC
    // e.g. old form 2013-06-26 22:38:00+00:00
    var tm time.Time
    oldLongForm    := "2006-01-02 15:04:05+00:00"
    newLongForm    := "2006-01-02 15:04:05 +0000 UTC"
    lineComponents := strings.Split(anomalyLine, ",")
    a.Id          = lineComponents[0]
    a.Anomaly     = lineComponents[1]
    a.DeviceId    = lineComponents[2]
    a.DevicePhase = lineComponents[3]
    a.DeviceType  = lineComponents[4]
    a.FeederId    = lineComponents[5]
    a.Signal      = lineComponents[6]
    if len(lineComponents) >= 9 { // use newLongForm
        a.Value   = lineComponents[7]
        a.Time    = lineComponents[8]
        tm, _     = time.Parse(newLongForm, a.Time)
    } else { // use oldLongForm
        a.Value   = "-"
        a.Time    = lineComponents[7]
        tm, _     = time.Parse(oldLongForm, a.Time)
    }
    a.EpochTime = tm.Unix()
}

func GetAnomalies(fileName string) map[string][]Anomaly {
    var anomaliesMap map[string][]Anomaly = make(map[string][]Anomaly)
    if file, err := os.Open(fileName); err == nil {
        defer file.Close()

        lineNum  := 0
        scanner  := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            if lineNum > 0 { // ignore header line
                if len(strings.Split(line, ",")) >= 7 {
                    anomaly := new(Anomaly)
                    anomaly.Create(line)
                    if _, ok := anomaliesMap[anomaly.FeederId]; !ok {
                        anomaliesMap[anomaly.FeederId] = make([]Anomaly, 0)
                    }
                    anomaliesMap[anomaly.FeederId] = append(anomaliesMap[anomaly.FeederId], *anomaly)
                }
            }
            if lineNum % 1000000 == 0 {
                // fmt.Printf("Reading %d ...\n", lineNum)
            }
            lineNum++
        }
        if err = scanner.Err(); err != nil {
            log.Fatal(err)
        }
    } else {
        log.Fatal(err)
    }
    return anomaliesMap
}

func TruncateAnomalyTimes(anomalies map[string][]Anomaly) {
    for feederId, _ := range anomalies {
        for i, _ := range anomalies[feederId] {
            remainder := anomalies[feederId][i].EpochTime % 60
            anomalies[feederId][i].EpochTime -= remainder
        }
    }
}

func (a *Anomaly) Format() string {
    return fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%d",
        a.Id, a.Anomaly, a.DeviceId, a.DevicePhase, a.DeviceType, a.FeederId, a.Signal, a.Value, a.Time, a.EpochTime)
}

