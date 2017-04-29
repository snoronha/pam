package lib

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "time"
)

func CompareAllAnomsWithEDNAAnoms() {
    oldLongForm := "2006-01-02 15:04:05+00:00"
    newLongForm := "2006-01-02 15:04:05 +0000 UTC"
    oldFileName := "/Users/sanjaynoronha/Desktop/all_anoms_feb2015.csv"
    newFileName := "/Users/sanjaynoronha/Desktop/edna_out.txt"
    // oldFileName := "/Users/sanjaynoronha/Desktop/all_anoms_811635.csv"
    // newFileName := "/Users/sanjaynoronha/Desktop/edna_811635.txt"

    oldMap := make(map[string]map[string]map[string]map[string]string)
    newMap := make(map[string]map[string]map[string]map[string]string)

    // create oldMap: oldMap[feederId][anomalyType][ts] = line
    startTime   := time.Now()

    fdrRegexp, _   := regexp.Compile(`\.([0-9]{6})[\._]`)
    phaseRegexp, _ := regexp.Compile(`\.([ABC\-])_PH`)
    loc, _         := time.LoadLocation("America/New_York")
    goodCount, badCount := 0, 0
    startTime   = time.Now()
    if newFile, err := os.Open(newFileName); err == nil {
        defer newFile.Close()
        numLines := 0
        newScanner  := bufio.NewScanner(newFile)
        for newScanner.Scan() {
            line := newScanner.Text()
            lineComponents := strings.Split(line, ",")
            if len(lineComponents) >= 2 {
                numLines++
                extendedId  := lineComponents[1]
                anomalyType := lineComponents[0]
                feederIdMatches := fdrRegexp.FindStringSubmatch(extendedId)
                feederId    := ""
                if len(feederIdMatches) > 0 {
                    feederId = feederIdMatches[1]
                }
                phaseMatches := phaseRegexp.FindStringSubmatch(extendedId)
                phase    := "-"
                if len(phaseMatches) > 0 {
                    phase = phaseMatches[1]
                }
                ts, _       := time.Parse(newLongForm, lineComponents[3])
                epochTs     := strconv.FormatInt(ts.Unix(), 10)
                if _, ok := newMap[feederId]; !ok {
                    newMap[feederId] = map[string]map[string]map[string]string{}
                }
                if _, ok := newMap[feederId][anomalyType]; !ok {
                    newMap[feederId][anomalyType] = map[string]map[string]string{}
                }
                if _, ok := newMap[feederId][anomalyType][phase]; !ok {
                    newMap[feederId][anomalyType][phase] = map[string]string{}
                }
                if _, ok := newMap[feederId][anomalyType][phase][epochTs]; !ok {
                    newMap[feederId][anomalyType][phase][epochTs] = line
                }
                if numLines % 1000000 == 0 {
                    fmt.Printf("%d: type:%s feederId:%s line:[%s] epochTs:%s\n", numLines, anomalyType, feederId, line, epochTs)
                }
                
            }
        }

        elapsed := time.Since(startTime)
        fmt.Printf("{numLines: %d, elapsed: %s}\n", numLines, elapsed)
        
        // check for errors
        if err = newScanner.Err(); err != nil {
            log.Fatal(err)
        }

    } else {
        log.Fatal(err)
    }

    
    if oldFile, err := os.Open(oldFileName); err == nil {
        defer oldFile.Close()
        numLines := 0
        oldScanner  := bufio.NewScanner(oldFile)
        for oldScanner.Scan() {
            line := oldScanner.Text()
            lineComponents := strings.Split(line, ",")
            if len(lineComponents) >= 7 {
                numLines++
                extendedId  := lineComponents[6]
                _ = extendedId
                anomalyType := lineComponents[1]
                phase       := lineComponents[3]
                feederId    := lineComponents[5]
                ts, _       := time.Parse(oldLongForm, lineComponents[7])
                _, offset   := ts.In(loc).Zone()
                ts           = ts.Add(time.Duration(offset) * time.Second) // timestamps are ET, convert to UTC
                epochTs     := strconv.FormatInt(ts.Unix(), 10)
                if _, ok := oldMap[feederId]; !ok {
                    oldMap[feederId] = map[string]map[string]map[string]string{}
                }
                if _, ok := oldMap[feederId][anomalyType]; !ok {
                    oldMap[feederId][anomalyType] = map[string]map[string]string{}
                }
                if _, ok := oldMap[feederId][anomalyType][phase]; !ok {
                    oldMap[feederId][anomalyType][phase] = map[string]string{}
                }
                if _, ok := oldMap[feederId][anomalyType][phase][epochTs]; !ok {
                    oldMap[feederId][anomalyType][phase][epochTs] = line
                }
                if _, ok := newMap[feederId][anomalyType][phase][epochTs]; ok {
                    goodCount++
                } else {
                    badCount++
                    if badCount % 1 == 0 {
                        // fmt.Printf("BAD: %d type:%s phase:%s [%s] epochTs:%s\n", numLines, anomalyType, phase, line, epochTs)
                    }
                }
                if numLines % 1000000 == 0 {
                    fmt.Printf("%d: type:%s [%s] epochTs:%s\n", numLines, anomalyType, line, epochTs)
                }
            }
        }

        elapsed := time.Since(startTime)
        fmt.Printf("{numLines: %d, goodCount: %d, badCount: %d, elapsed: %s}\n", numLines, goodCount, badCount, elapsed)

        var feederIds []string
        for feederId := range newMap {
            feederIds = append(feederIds, feederId)
        }
        sort.Strings(feederIds)
        for _, feederId := range feederIds {
            for fault := range newMap[feederId] {
                for phase := range newMap[feederId][fault] {
                    oldCount := len(oldMap[feederId][fault][phase])
                    newCount := len(newMap[feederId][fault][phase])
                    var absDiff int = 0
                    if oldCount > newCount {
                        absDiff = oldCount - newCount
                    } else {
                        absDiff = newCount - oldCount
                    }
                    if absDiff >= 100 {
                        fmt.Printf("[%s][%s][%s] = {old:%d, new:%d]\n", feederId, fault, phase, oldCount, newCount)
                    }
                }
            }
        }
        
        // check for errors
        if err = oldScanner.Err(); err != nil {
            log.Fatal(err)
        }

    } else {
        log.Fatal(err)
    }


}

// e.g. fileName = "/Users/<username>/edna_monthly", extension = ".csv"
func SortMergeAnomalyFile(newFilePath string, newExtension string, oldFilePath string, oldExtension string) {
    var anomObjects []Anomaly
    numLines := 0
    newLongForm := "2006-01-02 15:04:05 +0000 UTC"
    newFileName := newFilePath + newExtension
    oldLongForm := "2006-01-02 15:04:05+00:00"
    oldFileName := oldFilePath + oldExtension

    // Read, parse new file
    if newFile, err := os.Open(newFileName); err == nil {
        // make sure it gets closed
        defer newFile.Close()
        scanner  := bufio.NewScanner(newFile)
        for scanner.Scan() {
            line := scanner.Text()
            lineComponents := strings.Split(line, ",")
            if len(lineComponents) >= 5 {
                anom            := new(Anomaly)

                // 0,FCI_FAULT_ALARM,673113B,B,FCI,806731,IVES.806731.FCI.673113B.FAULT.B_PH,1,2013-12-05 15:41:26 +0000 UTC
                anom.Id          = lineComponents[0]
                anom.Anomaly     = lineComponents[1]
                anom.DeviceId    = lineComponents[2]
                anom.DevicePhase = lineComponents[3]
                anom.DeviceType  = lineComponents[4]
                anom.FeederId    = lineComponents[5]
                anom.Signal      = lineComponents[6]
                anom.Value       = lineComponents[7]
                anom.Time        = lineComponents[8]

                evntTs, _       := time.Parse(newLongForm, anom.Time)
                anom.EpochTime   = evntTs.Unix()
                anomObjects      = append(anomObjects, *anom)
                if numLines % 1000000 == 0 {
                    fmt.Printf("%d\tnew %s epoch: %d\n", numLines, anom.Time, anom.EpochTime)
                }
                numLines++
            }
        }
    } else {
        log.Fatal(err)
    }
    fmt.Printf("NumLines: %d\n", numLines)

    // Read, parse old file
    if oldFile, err := os.Open(oldFileName); err == nil {
        // make sure it gets closed
        defer oldFile.Close()
        scanner  := bufio.NewScanner(oldFile)
        for scanner.Scan() {
            line := scanner.Text()
            lineComponents := strings.Split(line, ",")
            if len(lineComponents) >= 5 {
                anom            := new(Anomaly)

                anom.Id          = lineComponents[0]
                anom.Anomaly     = lineComponents[1]
                anom.DeviceId    = lineComponents[2]
                anom.DevicePhase = lineComponents[3]
                anom.DeviceType  = lineComponents[4]
                anom.FeederId    = lineComponents[5]
                anom.Signal      = lineComponents[6]
                anom.Value       = "0"
                anom.Time        = lineComponents[7]

                evntTs, _       := time.Parse(oldLongForm, anom.Time)
                anom.EpochTime   = evntTs.Unix()
                anom.Time        = fmt.Sprintf("%s", evntTs)
                anomObjects      = append(anomObjects, *anom)

                if numLines % 100000 == 0 {
                    fmt.Printf("%d\told %s epoch: %d\n", numLines, anom.Time, anom.EpochTime)
                }
                numLines++
            }
        }
    } else {
        log.Fatal(err)
    }

    sort.Slice(anomObjects, func(i, j int) bool {
        return anomObjects[i].EpochTime < anomObjects[j].EpochTime
    })
    fmt.Printf("Sorting done!\n")

    // Write out sorted, merged files
    oFileName := newFilePath + "_merged" + newExtension
    var writer *bufio.Writer
    if ofile, err := os.Create(oFileName); err == nil {
        defer ofile.Close()
        writer = bufio.NewWriter(ofile)
    } else {
        log.Fatal(err)
    }
    line := ""
    for _, anom := range anomObjects {
        line = anom.Id + "," + anom.Anomaly + "," + anom.DeviceId + "," + anom.DevicePhase + "," + anom.DeviceType + "," + anom.FeederId + "," +
            anom.Signal + "," + anom.Value + "," + anom.Time
        writer.WriteString(fmt.Sprintf("%s\n", line))
    }
    writer.Flush()
}

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

func (a *Anomaly) Format() string {
    return fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%d",
        a.Id, a.Anomaly, a.DeviceId, a.DevicePhase, a.DeviceType, a.FeederId, a.Signal, a.Value, a.Time, a.EpochTime)
}
