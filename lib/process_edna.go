package lib

import (
    "bufio"
    "fmt"
    "io/ioutil"
    "log"
    "math"
    "os"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "time"
)

func ProcessEDNA(startFileNumber int, endFileNumber int, monthlyOrBulk string, awsOrLocal string) {
	var MAX_EDNA_KEYS int64 = 100000
    processEdnaAnomaly := map[string]bool{
        "AFS_ALARM_ALARM": true,  "AFS_GROUND_ALARM": true, "AFS_I_FAULT_FULL": true, "AFS_I_FAULT_TEMP": true, "AFS_I_FAULT_NEW": true,
        "FCI_FAULT_ALARM": true,  "FCI_I_FAULT_FULL": true, "FCI_I_FAULT_TEMP": true, "FCI_I_FAULT_NEW":  true,
        "ZERO_CURRENT_V3": true,  "ZERO_CURRENT_V4":  true,
        "ZERO_POWER_V3":   true,  "ZERO_POWER_V4":    true,
        "ZERO_VOLTAGE_V3": true,  "ZERO_VOLTAGE_V4":  true,
        "PF_SPIKES_V3":    true,  "THD_SPIKES_V3":    true,
    }
    var ednaAnomalyCount map[string]int = make(map[string]int)
    for k, _ := range processEdnaAnomaly {
        ednaAnomalyCount[k] = 0
    }

    var writer *bufio.Writer
	var odir string
	if awsOrLocal == "local" {
		odir   = "./output/"
	} else {
		odir   = "/home/ubuntu/go/src/anomaly/"
	}
	
    ofileName := odir + "edna_" + monthlyOrBulk + "_" + strconv.Itoa(startFileNumber) + "_" + strconv.Itoa(endFileNumber) + ".csv"
    if ofile, err := os.Create(ofileName); err == nil {
        defer ofile.Close()
        writer = bufio.NewWriter(ofile)
    } else {
        log.Fatal(err)
    }

    startTime := time.Now()
    fileNum   := 0
    if monthlyOrBulk == "monthly" {
		if awsOrLocal == "local" {
			dir       := "/Volumes/auto-grid-pam/DISK1/pam-monthly-anomalies"
			dirs, _   := ioutil.ReadDir(dir)
			for _, d  := range dirs {
				monthlyDir := dir + "/" + d.Name()
				files, _  := ioutil.ReadDir(monthlyDir)
				for _, f  := range files {
					filePath := monthlyDir + "/" + f.Name()
					if strings.Contains(f.Name(), ".csv") {
						if fileNum >= startFileNumber && (endFileNumber < 0 || fileNum <= endFileNumber) { // && strings.Contains(f.Name(), "803036.csv") {
							processEDNAFile(filePath, filePath, fileNum, writer, startTime, ednaAnomalyCount, processEdnaAnomaly)
							writer.Flush()
						}
						fileNum++
					}
				}
			}
		} else { // awsOrLocal == "aws"
			svc       := GetAWSService("us-west-2")
			bucket    := "pam-monthly-anomalies"
			objects   := GetAWSObjectNames(svc, bucket, MAX_EDNA_KEYS, "EDNA")
			ofileName := "current_file_" + strconv.Itoa(startFileNumber) + "_" + strconv.Itoa(endFileNumber) + ".csv"
			fmt.Printf("%d object names retrieved ...\n", len(objects))
			for _, fileName := range objects {
				if fileNum >= startFileNumber && (endFileNumber < 0 || fileNum <= endFileNumber) { // && strings.Contains(f.Name(), "803036.csv") {
					GetAWSFile(svc, bucket, fileName, ofileName)
					processEDNAFile(ofileName, fileName, fileNum, writer, startTime, ednaAnomalyCount, processEdnaAnomaly)
					writer.Flush()
				}
				fileNum++
			}
		}
    } else {
        dir       := "/Volumes/auto-grid-pam/DISK1/bulk_data/edna/response"
        files, _  := ioutil.ReadDir(dir)
        for _, f  := range files {
            filePath := dir + "/" + f.Name()
            if strings.Contains(f.Name(), ".csv") {
                if fileNum >= startFileNumber && (endFileNumber < 0 || fileNum <= endFileNumber) { // && strings.Contains(f.Name(), "100132.csv") {
                    processEDNAFile(filePath, filePath, fileNum, writer, startTime, ednaAnomalyCount, processEdnaAnomaly)
                    writer.Flush()
                }
                fileNum++
            }
        }
    }
}

func processEDNAFile(fileName string, fileTag string, fileNum int, writer *bufio.Writer,
	startTime time.Time, anomalyCount map[string]int, processAnomaly map[string]bool) {

    longForm := "1/2/2006 3:04:05 PM"

    // create Windows for moving windows
    var zeroCurrentWindows map[string]Window = make(map[string]Window)
    var zeroPowerWindows   map[string]Window = make(map[string]Window)
    var zeroVoltageWindows map[string]Window = make(map[string]Window)
    var pfSpikesWindows    map[string]Window = make(map[string]Window)
    var thdSpikesWindows   map[string]Window = make(map[string]Window)

    fdrRegexp, _   := regexp.Compile(`\.([0-9]{6})[\._]`)
    phaseRegexp, _ := regexp.Compile(`\.([ABC\-])_PH`)

    // open file
    if file, err := os.Open(fileName); err == nil {
        defer file.Close()

        // init counting variables
        var anomalies, filteredAnomalies []Anomaly
        var anomalyMap map[string]map[string]int64 = make(map[string]map[string]int64)
        numLines := 0
        for k, _ := range processAnomaly {
            anomalyMap[k] = make(map[string]int64)
        }

        // create a new scanner and read the file line by line
        scanner  := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            lineComponents := strings.Split(line, ",")
            if len(lineComponents) >= 5 {
                numLines++

                extendedId  := strings.Replace(lineComponents[0], "\"", "", -1)
                ts, _       := time.Parse(longForm, strings.Replace(lineComponents[1], "\"", "", -1))
                devicePhaseMatches := phaseRegexp.FindStringSubmatch(extendedId)
                devicePhase := "-"
                if len(devicePhaseMatches) > 0 {
                    devicePhase = devicePhaseMatches[1]
                }
                feederIdMatches := fdrRegexp.FindStringSubmatch(extendedId)
                feederId    := ""
                if len(feederIdMatches) > 0 {
                    feederId = feederIdMatches[1]
                }
                
                if strings.Contains(extendedId, ".AFS.") {
                    // handle potential AFS anomalies
                    deviceId := strings.Split(extendedId, ".")[3]
                    value, _ := strconv.Atoi(strings.Replace(lineComponents[2], "\"", "", -1))
                    valueString := fmt.Sprintf("%d", value)
                    if processAnomaly["AFS_ALARM_ALARM"] && strings.Contains(extendedId, ".ALARM") && strings.Contains(lineComponents[3], "ALARM") {
                        anomaly     := new(Anomaly)
                        anomaly.Populate("0", "AFS_ALARM_ALARM", deviceId, devicePhase, "AFS", feederId, extendedId, valueString, ts)
                        anomalies    = append(anomalies, *anomaly)
                    } else if processAnomaly["AFS_GROUND_ALARM"] && strings.Contains(extendedId, ".GROUND") && strings.Contains(lineComponents[3], "ALARM") {
                        anomaly     := new(Anomaly)
                        anomaly.Populate("0", "AFS_GROUND_ALARM", deviceId, devicePhase, "AFS", feederId, extendedId, valueString, ts)
                        anomalies    = append(anomalies, *anomaly)
                    } else if (processAnomaly["AFS_I_FAULT_FULL"] || processAnomaly["AFS_I_FAULT_TEMP"]) && strings.Contains(extendedId, ".I_FAULT") {                        
                        if value >= 600 {
                            if value >= 900 {
                                anomaly     := new(Anomaly)
                                anomaly.Populate("0", "AFS_I_FAULT_FULL", deviceId, devicePhase, "AFS", feederId, extendedId, valueString, ts)
                                anomalies    = append(anomalies, *anomaly)
                            } else {
                                anomaly     := new(Anomaly)
                                anomaly.Populate("0", "AFS_I_FAULT_TEMP", deviceId, devicePhase, "AFS", feederId, extendedId, valueString, ts)
                                anomalies    = append(anomalies, *anomaly)
                            }
                        }
                        if value >= 800 {
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "AFS_I_FAULT_NEW", deviceId, devicePhase, "AFS", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }
                    }
                }
                
                if strings.Contains(extendedId, ".FCI.") {
                    // handle potential FCI anomalies
                    value, _    := strconv.Atoi(strings.Replace(lineComponents[2], "\"", "", -1))
                    valueString := fmt.Sprintf("%d", value)
                    deviceId    := strings.Split(extendedId, ".")[3]
                    if processAnomaly["FCI_FAULT_ALARM"] && strings.Contains(extendedId, ".FAULT") && !strings.Contains(lineComponents[3], "NORMAL") {
                        anomalyCount["FCI_FAULT_ALARM"]++
                        writer.WriteString(fmt.Sprintf("0,FCI_FAULT_ALARM,%s,%s,FCI,%s,%s,%d,%s\n", deviceId, devicePhase, feederId, extendedId, value, ts))
                    } else if (processAnomaly["FCI_I_FAULT_FULL"] || processAnomaly["FCI_I_FAULT_TEMP"]) && strings.Contains(extendedId, ".I_FAULT") {
                        if value >= 600 {
                            deviceId := strings.Split(extendedId, ".")[3]
                            if value >= 900 {
                                anomaly     := new(Anomaly)
                                anomaly.Populate("0", "FCI_I_FAULT_FULL", deviceId, devicePhase, "FCI", feederId, extendedId, valueString, ts)
                                anomalies    = append(anomalies, *anomaly)
                            } else {
                                anomaly     := new(Anomaly)
                                anomaly.Populate("0", "FCI_I_FAULT_TEMP", deviceId, devicePhase, "FCI", feederId, extendedId, valueString, ts)
                                anomalies    = append(anomalies, *anomaly)
                            }
                        }
                        if value >= 800 {
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "FCI_I_FAULT_NEW", deviceId, devicePhase, "FCI", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }
                    }
                }

                if (processAnomaly["ZERO_CURRENT_V3"] || processAnomaly["ZERO_CURRENT_V4"]) &&
                    strings.Contains(extendedId, ".I.") && strings.Contains(extendedId, "_PH") &&
                    strings.Contains(extendedId, ".FDR.") && !strings.Contains(extendedId, "BKR.") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    valueString := fmt.Sprintf("%.3f", value)
                    _, ok := zeroCurrentWindows[extendedId]
                    if !ok {
                        zeroCurrentWindows[extendedId] = Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
                    }
                    zeroCurrentWindow := zeroCurrentWindows[extendedId]
                    zeroCurrentWindow.AddElement(ts, extendedId, value)
                    zeroCurrentWindow.SetStartPointer()
                    if value > -0.5 && value < 1 {
                        deviceId := strings.Split(strings.Split(extendedId, ".")[2], "_")[1]
                        if processAnomaly["ZERO_CURRENT_V3"] && zeroCurrentWindow.QuantileGreaterThanThreshold(0.01, 10.0, 24) {
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "ZERO_CURRENT_V3", deviceId, devicePhase, "PHASER", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }
                        prevPointer := zeroCurrentWindow.EndPointer - 1
                        if processAnomaly["ZERO_CURRENT_V4"] && zeroCurrentWindow.GreaterThanThreshold(prevPointer, 1.0) {
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "ZERO_CURRENT_V4", deviceId, devicePhase, "PHASER", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }

                        mean := zeroCurrentWindow.Mean()
                        _ = mean
                    }
                    zeroCurrentWindows[extendedId] = zeroCurrentWindow
                }

                if processAnomaly["PF_SPIKES_V3"] &&
                    strings.Contains(extendedId, ".PF.") && strings.Contains(extendedId, "_PH") &&
                    strings.Contains(extendedId, ".FDR.") && !strings.Contains(extendedId, "BKR.") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    _, ok := pfSpikesWindows[extendedId]
                    if !ok {
                        pfSpikesWindows[extendedId] = Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
                    }
                    pfSpikesWindow := pfSpikesWindows[extendedId]
                    pfSpikesWindow.AddElement(ts, extendedId, math.Abs(value))
                    pfSpikesWindow.SetStartPointer()
                    if math.Abs(value) < 0.75 {
                        deviceId := strings.Split(strings.Split(extendedId, ".")[2], "_")[1]
                        _  = deviceId
                        if pfSpikesWindow.QuantileGreaterThanThreshold(0.01, 0.8, 24) {
                            valueString := fmt.Sprintf("%.3f", value)
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "PF_SPIKES_V3", deviceId, devicePhase, "PHASER", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }
                    }
                    pfSpikesWindows[extendedId] = pfSpikesWindow
                }

                if (processAnomaly["ZERO_POWER_V3"] || processAnomaly["ZERO_POWER_V4"]) &&
                    strings.Contains(extendedId, ".MW") &&
                    strings.Contains(extendedId, ".FDR.") && !strings.Contains(extendedId, "BKR.") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    _, ok := zeroPowerWindows[extendedId]
                    if !ok {
                        zeroPowerWindows[extendedId] = Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
                    }
                    zeroPowerWindow := zeroPowerWindows[extendedId]
                    zeroPowerWindow.AddElement(ts, extendedId, value)
                    zeroPowerWindow.SetStartPointer()
                    if value > -0.5 && value < 0.1 {
                        deviceId    := "-"
                        deviceIdArr := strings.Split(strings.Split(extendedId, ".")[2], "_")
                        if len(deviceIdArr) >= 2 {
                            deviceId = strings.Split(strings.Split(extendedId, ".")[2], "_")[1]
                        }
                        valueString := fmt.Sprintf("%.3f", value)
                        if zeroPowerWindow.QuantileGreaterThanThreshold(0.01, 0.5, 24) {
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "ZERO_POWER_V3", deviceId, devicePhase, "PHASER", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }
                        prevPointer := zeroPowerWindow.EndPointer - 1
                        if zeroPowerWindow.GreaterThanThreshold(prevPointer, 0.1) {
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "ZERO_POWER_V4", deviceId, devicePhase, "PHASER", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }
                    }
                    zeroPowerWindows[extendedId] = zeroPowerWindow
                }

                if (processAnomaly["ZERO_VOLTAGE_V3"] || processAnomaly["ZERO_VOLTAGE_V4"]) &&
                    strings.Contains(extendedId, ".V.") && strings.Contains(extendedId, "_PH") &&
                    strings.Contains(extendedId, ".FDR.") && !strings.Contains(extendedId, "BKR.") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    _, ok := zeroVoltageWindows[extendedId]
                    if !ok {
                        zeroVoltageWindows[extendedId] = Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
                    }
                    zeroVoltageWindow := zeroVoltageWindows[extendedId]
                    zeroVoltageWindow.AddElement(ts, extendedId, value)
                    zeroVoltageWindow.SetStartPointer()
                    if value > -0.5 && value < 1.0 {
                        valueString := fmt.Sprintf("%.3f", value)
                        deviceId := strings.Split(strings.Split(extendedId, ".")[2], "_")[1]
                        if zeroVoltageWindow.QuantileGreaterThanThreshold(0.01, 90.0, 24) {
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "ZERO_VOLTAGE_V3", deviceId, devicePhase, "PHASER", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }
                        prevPointer := zeroVoltageWindow.EndPointer - 1
                        if zeroVoltageWindow.GreaterThanThreshold(prevPointer, 1.0) {
                            anomaly     := new(Anomaly)
                            anomaly.Populate("0", "ZERO_VOLTAGE_V4", deviceId, devicePhase, "PHASER", feederId, extendedId, valueString, ts)
                            anomalies    = append(anomalies, *anomaly)
                        }
                    }
                    zeroVoltageWindows[extendedId] = zeroVoltageWindow
                }

                if processAnomaly["THD_SPIKES_V3"] && strings.Contains(extendedId, ".THD_") && strings.Contains(extendedId, "urrent") {
                    value, _ := strconv.ParseFloat(strings.Replace(lineComponents[2], "\"", "", -1), 64)
                    _, ok := thdSpikesWindows[extendedId]
                    if !ok {
                        thdSpikesWindows[extendedId] = Window{StartPointer: 0, EndPointer: -1, MAXSIZE: 1000}
                    }
                    thdSpikesWindow := thdSpikesWindows[extendedId]
                    thdSpikesWindow.AddElement(ts, extendedId, value)
                    thdSpikesWindow.SetStartPointer()
                    mean      := thdSpikesWindow.Mean()
                    stdDev    := thdSpikesWindow.StdDeviation()
                    threshold := mean + 7.0 * stdDev
                    if value > threshold {
                        valueString := fmt.Sprintf("%.3f", value)
                        deviceId := strings.Split(strings.Split(extendedId, ".")[2], "_")[1]
                        anomaly     := new(Anomaly)
                        anomaly.Populate("0", "THD_SPIKES_V3", deviceId, devicePhase, "PHASER", feederId, extendedId, valueString, ts)
                        anomalies    = append(anomalies, *anomaly)
                    }
                    thdSpikesWindows[extendedId] = thdSpikesWindow
                }

                if numLines % 1000000 == 0 {
                    fmt.Printf("%d: idStr=[%s]\n", numLines, extendedId)
                }
            }
        }

        // sort anomalies by EpochTime, then DevicePhase
        sort.Slice(anomalies, func(i, j int) bool {
            if anomalies[i].EpochTime == anomalies[j].EpochTime {
                return (strings.Compare(anomalies[i].DevicePhase, anomalies[j].DevicePhase) <= 0)
            } else {
                return anomalies[i].EpochTime < anomalies[j].EpochTime
            }
        })
        // filter anomalies with the same extendedId and within 3 minutes
        for _, anomaly := range anomalies {
            anomalyType := anomaly.Anomaly
            if _, ok := anomalyMap[anomalyType][anomaly.Signal]; !ok {
                anomalyMap[anomalyType][anomaly.Signal] = anomaly.EpochTime
                filteredAnomalies = append(filteredAnomalies, anomaly)
                anomalyCount[anomalyType]++
            } else {
                if anomaly.EpochTime - anomalyMap[anomalyType][anomaly.Signal] >= 180 { // difference is > 3 minutes
                    anomalyMap[anomalyType][anomaly.Signal] = anomaly.EpochTime
                    filteredAnomalies = append(filteredAnomalies, anomaly)
                    anomalyCount[anomalyType]++
                }
            }
        }

        // write out filtered anomalies
        for _, anomaly := range filteredAnomalies {
            writer.WriteString(fmt.Sprintf("%s\n", anomaly.Format()))
        }
        
        anomalyStr := ""
        for k, v  := range anomalyCount {
            if processAnomaly[k] {
                anomalyStr += ", " + k + ": " + strconv.Itoa(v)
            }
        }
        elapsed := time.Since(startTime)
        fmt.Printf("{id: %d, filePath: \"%s\", numLines: %d, elapsed: %s%s}\n", fileNum, fileTag, numLines, elapsed, anomalyStr)
        
        // check for errors
        if err = scanner.Err(); err != nil {
            log.Fatal(err)
        }

    } else {
        log.Fatal(err)
    }
}

