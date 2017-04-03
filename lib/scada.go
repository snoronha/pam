package lib

import (
    "bufio"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "strconv"
    "strings"
    "time"
)

func ProcessSCADA() {
    scadaAnomalyCount := map[string]int{
        "BKR_CLOSE":           0, "BKR_FAIL_TO_OPR":      0, "BKR_OPEN":     0, "CURRENT_LIMIT": 0,
        "FAULT_ALARM":         0, "FAULT_CURRENT":        0, "FC_NO_BO":     0,
        "FDRHD_DE_ENERGIZED":  0, "FDRHD_ENERGIZED":      0, "HIGH_VOLTAGE": 0,
        "INTELI_PH_ALARM":     0, "INTELI_OPS_DSW_CLOSE": 0,
        "INTELI_OPS_DSW_OPEN": 0, "REGULATOR_BLOCK":      0, "RELAY_ALARM":  0,
        "RELAY_TRIP":          0, "TEMP_FAULT_CURRENT":   0, "VOLTAGE_DROP": 0,
    }
    processScadaAnomaly := map[string]bool{
        "BKR_CLOSE":           true, "BKR_FAIL_TO_OPR":      true, "BKR_OPEN":     true, "CURRENT_LIMIT": true,
        "FAULT_ALARM":         true, "FAULT_CURRENT":        true, "FC_NO_BO":     true,
        "FDRHD_DE_ENERGIZED":  true, "FDRHD_ENERGIZED":      true, "HIGH_VOLTAGE": true,
        "INTELI_PH_ALARM":     true, "INTELI_OPS_DSW_CLOSE": true,
        "INTELI_OPS_DSW_OPEN": true, "REGULATOR_BLOCK":      true, "RELAY_ALARM":  true,
        "RELAY_TRIP":          true, "TEMP_FAULT_CURRENT":   true, "VOLTAGE_DROP": true,
    }

    var writer *bufio.Writer
    ofileName := "/Users/sanjaynoronha/Desktop/scada_out.txt"
    if ofile, err := os.Create(ofileName); err == nil {
        defer ofile.Close()
        writer = bufio.NewWriter(ofile)
    } else {
        log.Fatal(err)
    }

    startTime := time.Now()
    dir       := "/Volumes/auto-grid-pam/DISK1/bulk_data/scada"
    files, _  := ioutil.ReadDir(dir)
    fileNum   := 0
    for _, f  := range files {
        filePath := dir + "/" + f.Name()
        if fileNum >= 0 {
            processSCADAFile(filePath, fileNum, writer, startTime, scadaAnomalyCount, processScadaAnomaly)
            writer.Flush()
        }
        fileNum++
    }
}

func processSCADAFile(fileName string, fileNum int, writer *bufio.Writer, startTime time.Time,  anomalyCount map[string]int, processAnomaly map[string]bool) {
    // OBSERV_KEY,OBSERV_TIMESTAMP_GMT,localTime,OBSERV_DATA,OBSERV_SCADA_FAC_NAME,AMP_NM_FAC,OBSERV_EQUIP_TYPE,OBSERV_EQUIP_ID,substationId,feederNumber,OBSERV_EQUIP_ACTION,OBSERV_SOURCE,OBSERV_FTO_FLAG,OBSERV_CREATE_TS,OBSERV_LFO_TIMESTAMP,OBSERV_AOR,DATE_KEY_GMT,DATE_KEY_LOCAL,
    // "35527944","2012-01-05 21:49:44.0","2012-01-05 16:49:44.0","DAYTONA_BEACH FEEDER 2W160_F0131 BKR OPEN-CLOSED","DATNA_BC","Daytona Beach","FEEDER BKR","2W160_F0131","52","100131","OPEN-CLOSE","DIST","false","2012-01-05 16:49:38.547","2012-01-05 21:50:59.0","DYDAYTNS","7443","7443",
    // Magic date in format of input file. Used for date parsing
    longForm := "2006-01-02 15:04:05"

    // open file
    if file, err := os.Open(fileName); err == nil {
        // make sure it gets closed
        defer file.Close()

        // init counting variables
        numLines := 0

        // create a new scanner and read the file line by line
        scanner  := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            lineComponents := strings.Split(line, ",")
            if len(lineComponents) >= 16 {
                numLines++

                observData   := lineComponents[3]
                observTs, _  := time.Parse(longForm, strings.Replace(lineComponents[1], "\"", "", -1))
                _ = observTs

                if strings.Contains(observData, "FEED") && strings.Contains(observData, "BKR") {
                    // handle potential BKR anomalies
                    if processAnomaly["BKR_OPEN"] && strings.Contains(observData, "OPEN") {
                        anomalyCount["BKR_OPEN"] += 1
                    } else if processAnomaly["BKR_CLOSE"] && strings.Contains(observData, "CLOSE") {
                        anomalyCount["BKR_CLOSE"] += 1
                    } else if processAnomaly["BKR_OPEN"] && strings.Contains(observData, "OPEN_CLOSE_OPEN") {
                        anomalyCount["BKR_OPEN"] += 1
                    } else if processAnomaly["BKR_CLOSE"] && strings.Contains(observData, "CLOSE_OPEN_CLOSE") {
                        anomalyCount["BKR_CLOSE"] += 1
                    } else if processAnomaly["BKR_FAIL_TO_OPR"] && strings.Contains(observData, "FAIL_TO_OPR") {
                        anomalyCount["BKR_FAIL_TO_OPR"] += 1
                    }
                }

                if strings.Contains(observData, " FAULT ") && strings.Contains(observData, " ALARM ") {
                    anomalyCount["FAULT_ALARM"] += 1
                }

                if strings.Contains(observData, "LIM-HIGH") {
                    observDataComponents := strings.Split(observData, " ")
                    if len(observDataComponents) >= 6 {
                        value, _ := strconv.ParseFloat(observDataComponents[5], 64)
                        if value >= 900 {
                            anomalyCount["FAULT_CURRENT"] += 1
                        } else {
                            anomalyCount["TEMP_FAULT_CURRENT"] += 1
                        }
                    }
                }

                if strings.Contains(observData, "AMP LIM-1 HIGH") && strings.Contains(observData, "ALARM") {
                    anomalyCount["CURRENT_LIMIT"] += 1
                }

                if strings.Contains(observData, " FDRHD ") {
                    if strings.Contains(observData, "ENGZ ENERGIZED") {
                        anomalyCount["FDRHD_ENERGIZED"] += 1
                    } else if strings.Contains(observData, "ENGZ DE-ENERGIZED") {
                        anomalyCount["FDRHD_DE_ENERGIZED"] += 1
                    }
                }

                if (strings.Contains(observData, "VLT LIM") || strings.Contains(observData, "VT LIM")) &&
                    strings.Contains(observData, "HIGH") {
                    observDataComponents := strings.Split(observData, " ")
                    if len(observDataComponents) >= 7 {
                        value, _ := strconv.ParseFloat(observDataComponents[6], 64)
                        if value >= 130 && value < 1000 {
                            anomalyCount["HIGH_VOLTAGE"] += 1
                        }
                    }
                }

                if strings.Contains(observData, " INTELI ") && strings.Contains(observData, "PH ALARM") {
                    anomalyCount["INTELI_PH_ALARM"] += 1
                }

                if strings.Contains(observData, " INTELI ") && strings.Contains(observData, "DSW") {
                    if strings.Contains(observData, "OPEN") {
                        anomalyCount["INTELI_OPS_DSW_OPEN"] += 1
                    } else if strings.Contains(observData, "CLOSE") {
                        anomalyCount["INTELI_OPS_DSW_CLOSE"] += 1
                    }
                }

                if strings.Contains(observData, " FDRHD ") && strings.Contains(observData, " REGU ") &&
                    strings.Contains(observData, "BLOCK") {
                    anomalyCount["REGULATOR_BLOCK"] += 1
                }
                
                if strings.Contains(observData, " RELAY ") && !strings.Contains(observData, "NORMAL") &&
                    !strings.Contains(observData, "STATUS") {
                    if strings.Contains(observData, "ALARM") {
                        anomalyCount["RELAY_ALARM"] += 1
                    } else if strings.Contains(observData, "TRIP") {
                        anomalyCount["RELAY_TRIP"] += 1
                    }
                }

                if strings.Contains(observData, "FORBDN") {
                    anomalyCount["VOLTAGE_DROP"] += 1
                }
                    
            }
        }

        anomalyStr := ""
        for k, v  := range anomalyCount {
            if processAnomaly[k] {
                anomalyStr += ", " + k + ": " + strconv.Itoa(v)
            }
        }
        elapsed := time.Since(startTime)
        fmt.Printf("{id: %d, filePath: \"%s\", numLines: %d, elapsed: %s%s}\n", fileNum, fileName, numLines, elapsed, anomalyStr)
        
        // check for errors
        if err = scanner.Err(); err != nil {
            log.Fatal(err)
        }

    } else {
        log.Fatal(err)
    }
}
