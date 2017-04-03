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

func ProcessEDNA() {
    ednaAnomalyCount := map[string]int{
        "AFS_ALARM_ALARM": 0, "AFS_GROUND_ALARM": 0, "AFS_I_FAULT_FULL": 0, "AFS_I_FAULT_TEMP": 0,
        "FCI_FAULT_ALARM": 0, "FCI_I_FAULT_FULL": 0, "FCI_I_FAULT_TEMP": 0,
        "ZERO_CURRENT_V3": 0, "ZERO_CURRENT_V4":  0,
        "ZERO_POWER_V3":   0, "ZERO_POWER_V4":    0,
        "ZERO_VOLTAGE_V3": 0, "ZERO_VOLTAGE_V4":  0,
        "PF_SPIKES_V3":    0, "THD_SPIKES_V3":    0,
    }
    processEdnaAnomaly := map[string]bool{
        "AFS_ALARM_ALARM": true,  "AFS_GROUND_ALARM": true, "AFS_I_FAULT_FULL": true, "AFS_I_FAULT_TEMP": true,
        "FCI_FAULT_ALARM": true,  "FCI_I_FAULT_FULL": true, "FCI_I_FAULT_TEMP": true,
        "ZERO_CURRENT_V3": true,  "ZERO_CURRENT_V4":  true,
        "ZERO_POWER_V3":   true,  "ZERO_POWER_V4":    true,
        "ZERO_VOLTAGE_V3": true,  "ZERO_VOLTAGE_V4":  true,
        "PF_SPIKES_V3":    true,  "THD_SPIKES_V3":    true,
    }

    var writer *bufio.Writer
    ofileName := "/Users/sanjaynoronha/Desktop/edna_out.txt"
    if ofile, err := os.Create(ofileName); err == nil {
        defer ofile.Close()
        writer = bufio.NewWriter(ofile)
    } else {
        log.Fatal(err)
    }

    startTime := time.Now()
    dir       := "/Volumes/auto-grid-pam/DISK1/bulk_data/edna/response"
    files, _  := ioutil.ReadDir(dir)
    fileNum   := 0
    for _, f  := range files {
        filePath := dir + "/" + f.Name()
        if strings.Contains(f.Name(), ".csv") {
            // if fileNum > 222 {
            if fileNum >= 0 {
                processEDNAFile(filePath, fileNum, writer, startTime, ednaAnomalyCount, processEdnaAnomaly)
                writer.Flush()
            }
            fileNum++
        }
    }
}

func processEDNAFile(fileName string, fileNum int, writer *bufio.Writer, startTime time.Time,  anomalyCount map[string]int, processAnomaly map[string]bool) {
    // Magic date in format of input file. Used for date parsing
    longForm := "1/2/2006 3:04:05 PM"

    // create Windows for moving windows
    zeroCurrentWindow := Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
    zeroPowerWindow   := Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
    zeroVoltageWindow := Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
    pfSpikesWindow    := Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
    
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
            if len(lineComponents) >= 5 {
                numLines++

                // Fail good data as early as possible
                extendedId := lineComponents[0]
                ts, _      := time.Parse(longForm, strings.Replace(lineComponents[1], "\"", "", -1))
                _ = ts

                
                if strings.Contains(extendedId, ".AFS.") {
                    // handle potential AFS anomalies
                    if processAnomaly["AFS_ALARM_ALARM"] && strings.Contains(extendedId, ".ALARM") && strings.Contains(lineComponents[3], "ALARM") {
                        anomalyCount["AFS_ALARM_ALARM"] += 1
                    } else if processAnomaly["AFS_GROUND_ALARM"] && strings.Contains(extendedId, ".GROUND") && strings.Contains(lineComponents[3], "ALARM") {
                        anomalyCount["AFS_GROUND_ALARM"]++
                    } else if (processAnomaly["AFS_I_FAULT_FULL"] || processAnomaly["AFS_I_FAULT_TEMP"]) && strings.Contains(extendedId, ".I_FAULT") {
                        value, _ := strconv.Atoi(strings.Replace(lineComponents[2], "\"", "", -1))
                        if value >= 600 {
                            if value >= 900 {
                                anomalyCount["AFS_I_FAULT_FULL"]++
                                writer.WriteString(fmt.Sprintf("{type:\"AFS_I_FAULT_FULL\",extendedId:%s,value:%d,ts:\"%s\"}\n", extendedId, value, ts))
                            } else {
                                anomalyCount["AFS_I_FAULT_TEMP"]++
                                writer.WriteString(fmt.Sprintf("{type:\"AFS_I_FAULT_TEMP\",extendedId:%s,value:%d,ts:\"%s\"}\n", extendedId, value, ts))
                            }
                        }
                    }
                }
                
                if strings.Contains(extendedId, ".FCI.") {
                    // handle potential FCI anomalies
                    if processAnomaly["FCI_FAULT_ALARM"] && strings.Contains(extendedId, ".FAULT") && !strings.Contains(lineComponents[3], "NORMAL") {
                        anomalyCount["FCI_FAULT_ALARM"]++
                    } else if (processAnomaly["FCI_I_FAULT_FULL"] || processAnomaly["FCI_I_FAULT_TEMP"]) && strings.Contains(extendedId, ".I_FAULT") {
                        value, _ := strconv.Atoi(strings.Replace(lineComponents[2], "\"", "", -1))
                        if value >= 600 {
                            if value >= 900 {
                                anomalyCount["FCI_I_FAULT_FULL"]++
                                writer.WriteString(fmt.Sprintf("{type:\"FCI_I_FAULT_FULL\",extendedId:%s,value:%d,ts:\"%s\"}\n", extendedId, value, ts))
                            } else {
                                anomalyCount["FCI_I_FAULT_TEMP"]++;
                                writer.WriteString(fmt.Sprintf("{type:\"FCI_I_FAULT_TEMP\",extendedId:%s,value:%d,ts:\"%s\"}\n", extendedId, value, ts))
                            }
                        }
                    }
                }

                if (processAnomaly["ZERO_CURRENT_V3"] || processAnomaly["ZERO_CURRENT_V4"]) &&
                    strings.Contains(extendedId, ".I.") && strings.Contains(extendedId, "_PH") &&
                    strings.Contains(extendedId, ".FDR.") && strings.Contains(extendedId, "BKR.") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    zeroCurrentWindow.AddElement(ts, extendedId, value)
                    zeroCurrentWindow.SetStartPointer()
                    if value > -0.5 && value < 1 {
                        if zeroCurrentWindow.QuantileGreaterThanThreshold(0.01, 10.0) {
                            anomalyCount["ZERO_CURRENT_V3"]++
                            writer.WriteString(fmt.Sprintf("{type:\"ZERO_CURRENT_V3\",extendedId:%s,value:%f,ts:\"%s\"}\n", extendedId, value, ts))
                        }
                        prevPointer := zeroCurrentWindow.EndPointer - 1
                        if zeroCurrentWindow.GreaterThanThreshold(prevPointer, 1.0) {
                            anomalyCount["ZERO_CURRENT_V4"]++
                            writer.WriteString(fmt.Sprintf("{type:\"ZERO_CURRENT_V4\",extendedId:%s,value:%f,ts:\"%s\"}\n", extendedId, value, ts))
                        }

                        mean := zeroCurrentWindow.Mean()
                        _ = mean
                        if numLines % 100 == 0 {
                            // fmt.Printf("%d: [%d, %d] value: %.2f mean: %.3f\n", numLines, zeroCurrentWindow.startPointer, zeroCurrentWindow.EndPointer, value, mean)
                        }
                    }
                }

                if processAnomaly["PF_SPIKES_V3"] &&
                    strings.Contains(extendedId, ".PF.") && strings.Contains(extendedId, "_PH") &&
                    strings.Contains(extendedId, ".FDR.") && strings.Contains(extendedId, "BKR.") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    zeroPowerWindow.AddElement(ts, extendedId, value)
                    zeroPowerWindow.SetStartPointer()
                    if value < 0.75 {
                        if pfSpikesWindow.QuantileGreaterThanThreshold(0.01, 0.8) {
                            anomalyCount["PF_SPIKES_V3"]++
                        }
                    }
                }

                if (processAnomaly["ZERO_POWER_V3"] || processAnomaly["ZERO_POWER_V4"]) &&
                    strings.Contains(extendedId, ".MW") &&
                    strings.Contains(extendedId, ".FDR.") && strings.Contains(extendedId, "BKR.") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    zeroPowerWindow.AddElement(ts, extendedId, value)
                    zeroPowerWindow.SetStartPointer()
                    if value > -0.5 && value < 0.1 {
                        if zeroPowerWindow.QuantileGreaterThanThreshold(0.01, 0.5) {
                            anomalyCount["ZERO_POWER_V3"]++
                            writer.WriteString(fmt.Sprintf("{type:\"ZERO_POWER_V3\",extendedId:%s,value:%f,ts:\"%s\"}\n", extendedId, value, ts))
                        }
                        prevPointer := zeroPowerWindow.EndPointer - 1
                        if zeroPowerWindow.GreaterThanThreshold(prevPointer, 0.1) {
                            anomalyCount["ZERO_POWER_V4"]++
                            writer.WriteString(fmt.Sprintf("{type:\"ZERO_POWER_V4\",extendedId:%s,value:%f,ts:\"%s\"}\n", extendedId, value, ts))
                        }
                    }
                }

                if (processAnomaly["ZERO_VOLTAGE_V3"] || processAnomaly["ZERO_VOLTAGE_V4"]) &&
                    strings.Contains(extendedId, ".V.") && strings.Contains(extendedId, "_PH") &&
                    strings.Contains(extendedId, ".FDR.") && strings.Contains(extendedId, "BKR.") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    zeroVoltageWindow.AddElement(ts, extendedId, value)
                    zeroVoltageWindow.SetStartPointer()
                    if value > -0.5 && value < 1.0 {
                        if zeroVoltageWindow.QuantileGreaterThanThreshold(0.01, 90.0) {
                            anomalyCount["ZERO_VOLTAGE_V3"]++
                            writer.WriteString(fmt.Sprintf("{type:\"ZERO_VOLTAGE_V3\",extendedId:%s,value:%f,ts:\"%s\"}\n", extendedId, value, ts))
                        }
                        prevPointer := zeroVoltageWindow.EndPointer - 1
                        if zeroVoltageWindow.GreaterThanThreshold(prevPointer, 1.0) {
                            anomalyCount["ZERO_VOLTAGE_V4"]++
                            writer.WriteString(fmt.Sprintf("{type:\"ZERO_VOLTAGE_V4\",extendedId:%s,value:%f,ts:\"%s\"}\n", extendedId, value, ts))
                        }
                    }
                }

                if processAnomaly["THD_SPIKES_V3"] && strings.Contains(extendedId, ".THD_") && strings.Contains(extendedId, "urrent") {
                    anomalyCount["THD_SPIKES_V3"]++;
                }

                if numLines % 1000000 == 0 {
                    // fmt.Printf("idStr=[%s] %s\n", ExtendedId, t)
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
