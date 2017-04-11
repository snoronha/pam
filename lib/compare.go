package lib

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "regexp"
    "strconv"
    "strings"
    "time"
)

func CompareAllAnomsWithEDNAAnoms() {
    oldLongForm := "2006-01-02 15:04:05+00:00"
    newLongForm := "2006-01-02 15:04:05 +0000 UTC"
    oldFileName := "/Users/sanjaynoronha/Desktop/all_anoms_feb2015.csv"
    newFileName := "/Users/sanjaynoronha/Desktop/edna_out.txt"
    // oldFileName := "/Users/sanjaynoronha/Desktop/all_anoms_805432.csv"
    // newFileName := "/Users/sanjaynoronha/Desktop/edna_805432.txt"

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

        for feederId := range newMap {
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
                        fmt.Printf("[%s][%s][%s] = {old:%d, new:%d]\n", feederId, fault, phase, len(oldMap[feederId][fault][phase]), len(newMap[feederId][fault][phase]))
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
