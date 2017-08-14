package lib

import (
    "bufio"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "time"
)

func ProcessAMI(startFileNumber int, endFileNumber int, monthlyOrBulk string, awsOrLocal string) {
    var MAX_AMI_KEYS int64 = 100000
    amiAnomalyCount   := map[string]int{ "LG_PD_10": 0, "LG_PD_10_V2": 0, }

    // Read customer data from csv dump
    var customerMap map[string]int64 

    // output file writer - handles AWS/local
    var writer *bufio.Writer
	var odir string
	if awsOrLocal == "local" {
        odir   = "/Users/sanjaynoronha/Desktop/"
        customerMap = readFeederMetadata("/Users/sanjaynoronha/go/src/anomaly/data/feeder_metadata.csv")
	} else {
        odir   = "/home/ubuntu/go/src/anomaly/"
        customerMap = readFeederMetadata("//home/ubuntu/go/src/anomaly/data/feeder_metadata.csv")
	}

    // create output file writer
    ofileName := odir + "ami_" + monthlyOrBulk + "_" + strconv.Itoa(startFileNumber) + "_" + strconv.Itoa(endFileNumber) + ".csv"
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
                            processAMIFile(filePath, filePath, fileNum, writer, startTime, amiAnomalyCount, customerMap, monthlyOrBulk)
                            writer.Flush()
                        }
                        fileNum++
                    }
                }
            }
        } else { // awsOrLocal == "aws"
            svc       := GetAWSService("us-west-2")
            bucket    := "pam-monthly-anomalies"
            objects   := GetAWSObjectNames(svc, bucket, MAX_AMI_KEYS, "AMI")
            ofileName := "current_file_" + strconv.Itoa(startFileNumber) + "_" + strconv.Itoa(endFileNumber) + ".csv"
            fmt.Printf("%d object names retrieved ...\n", len(objects))
            for _, fileName := range objects {
                if fileNum >= startFileNumber && (endFileNumber < 0 || fileNum <= endFileNumber) { // && strings.Contains(f.Name(), "803036.csv") {
                    GetAWSFile(svc, bucket, fileName, ofileName)
                    processAMIFile(ofileName, fileName, fileNum, writer, startTime, amiAnomalyCount, customerMap, monthlyOrBulk)
                    writer.Flush()
                }
                fileNum++
            }
        }
    } else {
        dir       := "/Volumes/auto-grid-pam/DISK1/bulk_data/ami"
        files, _  := ioutil.ReadDir(dir)
        for _, f  := range files {
            filePath := dir + "/" + f.Name()
            if strings.Contains(f.Name(), ".csv") {
                if fileNum >= startFileNumber && (endFileNumber < 0 || fileNum <= endFileNumber) { // && strings.Contains(f.Name(), "ami_100231.csv") {
                    processAMIFile(filePath, filePath, fileNum, writer, startTime, amiAnomalyCount, customerMap, monthlyOrBulk)
                    writer.Flush()
                }
                fileNum++
            }
        }
    }
}


func processAMIFile(fileName string, fileTag string, fileNum int, writer *bufio.Writer,
    startTime time.Time, anomalyCount map[string]int, customerMap map[string]int64, monthlyOrBulk string) {
    longForm := "2006-01-02 15:04:05"
    monthlyLongForm := "1/2/2006 3:04:05 PM"
	
    // open file
    if file, err := os.Open(fileName); err == nil {
        defer file.Close()

        var lgPd10 []int64
        _ = lgPd10
        numLines    := 0
        numAmiLines := 0
        var amiObjects []AMI
        hashMap     := make(map[int64]map[string][]AMI)

        mtrTmstmpRegexp, _   := regexp.Compile(`([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})`) // 2014-08-04 12:49:39-04

        // create a new scanner and read the file line by line
        scanner  := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            lineComponents := strings.Split(line, ",")
            if len(lineComponents) >= 11 {
                numLines++

                ami              := new(AMI)

                ami.SubstnName    = strings.Replace(lineComponents[0], "\"", "", -1)
                ami.FdrNum        = strings.Replace(lineComponents[1], "\"", "", -1)
                ami.PremNum       = strings.Replace(lineComponents[2], "\"", "", -1)
                ami.PhasType      = strings.Replace(lineComponents[3], "\"", "", -1)
                ami.CisDvcCoor    = strings.Replace(lineComponents[4], "\"", "", -1)
                ami.AmiDvcName    = strings.Replace(lineComponents[5], "\"", "", -1)
                ami.MtrEvntId     = strings.Replace(lineComponents[6], "\"", "", -1)
                ami.MtrEvntTmstmp = strings.Replace(lineComponents[7], "\"", "", -1)
                ami.EvntTxt       = strings.Replace(strings.Join(lineComponents[10:len(lineComponents)], ","), "\"", "", -1)

                modTmstmp := ""
                if strings.HasPrefix(ami.AmiDvcName, "G") &&
                    (strings.Contains(ami.MtrEvntId, "12007") || strings.Contains(ami.MtrEvntId, "12024")) {
                    numAmiLines++
                    if monthlyOrBulk == "bulk" {
                        matches := mtrTmstmpRegexp.FindStringSubmatch(ami.MtrEvntTmstmp)
                        if len(matches) > 0 {
                            modTmstmp = matches[1] + "-" + matches[2] + "-" + matches[3] + " " + matches[4] + ":" + matches[5] + ":00"
                            evntTs, _ := time.Parse(longForm, modTmstmp)
                            ami.MtrEvntEpoch = evntTs.Unix()
                        }
                    } else {
                        evntTs, _ := time.Parse(monthlyLongForm, ami.MtrEvntTmstmp)
                        ami.MtrEvntEpoch = evntTs.Unix()
                    }
                    amiObjects  = append(amiObjects, *ami)
                    if _, ok := hashMap[ami.MtrEvntEpoch]; !ok {
                        hashMap[ami.MtrEvntEpoch] = make(map[string][]AMI)
                    }
                    if _, ok := hashMap[ami.MtrEvntEpoch][ami.AmiDvcName]; !ok {
                        hashMap[ami.MtrEvntEpoch][ami.AmiDvcName] = make([]AMI, 0)
                    }
                    hashMap[ami.MtrEvntEpoch][ami.AmiDvcName] = append(hashMap[ami.MtrEvntEpoch][ami.AmiDvcName], *ami)

                }
            }
        }
        if len(amiObjects) <= 0 {
            return
        }

        sort.Slice(amiObjects, func(i, j int) bool {
            return amiObjects[i].MtrEvntEpoch < amiObjects[j].MtrEvntEpoch
        })

        var gasps []int64
        for epoch := range hashMap {
            if len(hashMap[epoch]) > 1 {
                gasps = append(gasps, epoch)
            }
        }
        sort.Slice(gasps, func(i, j int) bool {
            return gasps[i] < gasps[j]
        })
        var gaspsUnique  []int64
        var prev int64 = 0
        for _, epoch := range gasps {
            if epoch != prev {
                gaspsUnique = append(gaspsUnique, epoch)
                prev = epoch
            }
        }
        // fmt.Printf("\nGASPS LEN = %d LEN_UNIQUE: %d\n", len(gasps), len(gaspsUnique))
        fdrNum := amiObjects[0].FdrNum
        for i, t := range gaspsUnique {
            var nearbyGasps []int64
            k    := i
            done := false
            for k < len(gaspsUnique) && !done {
                if gaspsUnique[k] - t <= 300 { // 5 minutes
                    nearbyGasps = append(nearbyGasps, gaspsUnique[k])
                }
                k++
            }
            gaspMeters := make(map[string]bool)
            for _, t2 := range nearbyGasps {
                for dvcName := range hashMap[t2] {
                    if _, ok := gaspMeters[dvcName]; !ok {
                        gaspMeters[dvcName] = true
                    }
                }
            }
            gaspCount := len(gaspMeters)
            if len(nearbyGasps) > 0 || gaspCount > 0 {
                customerCount := customerMap[fdrNum]
                gaspPct := float64(gaspCount) / float64(customerCount)
                if gaspPct > 0.1 {
                    // fmt.Printf("len(nearbyGasps): %d, gaspCount: %d, customerCount: %d\n", len(nearbyGasps), gaspCount, customerCount)
                    anom := fmt.Sprintf("LAST GASPS / POWER DOWNS AT %.1f%% OF FEEDER CUSTOMERS (%d METERS)", (100 * gaspPct), gaspCount)
                    ts   := time.Unix(t, 0).UTC()
                    writer.WriteString(fmt.Sprintf("0,LG_PD_10,-,-,AMI,%s,%s,-,%s\n", fdrNum, anom, ts))
                    anomalyCount["LG_PD_10"]++;
                }
            }

            // Compute LG_PD_10_V2
            gaspMetersV2 := make(map[string]bool)
            for dvcName := range hashMap[t] {
                if _, ok := gaspMetersV2[dvcName]; !ok {
                    gaspMetersV2[dvcName] = true
                }
            }
            gaspCountV2 := len(gaspMetersV2)
            if gaspCountV2 > 0 {
                customerCount := customerMap[fdrNum]
                gaspPctV2 := float64(gaspCountV2) / float64(customerCount)
                if gaspPctV2 > 0.1 {
                    anom := fmt.Sprintf("LAST GASPS / POWER DOWNS AT %.1f%% OF FEEDER CUSTOMERS (%d METERS)", (100 * gaspPctV2), gaspCountV2)
                    ts   := time.Unix(t, 0).UTC()
                    writer.WriteString(fmt.Sprintf("0,LG_PD_10_V2,-,-,AMI,%s,%s,-,%s\n", fdrNum, anom, ts))
                    anomalyCount["LG_PD_10_V2"]++;
                }
            }
            
        }

        anomalyStr := ""
        for k, v  := range anomalyCount {
            anomalyStr += ", " + k + ": " + strconv.Itoa(v)
        }
        elapsed := time.Since(startTime)
        fmt.Printf("id: %d, fileName: %s, numLines: %d, elapsed: %s%s}\n", fileNum, fileName, numLines, elapsed, anomalyStr)
        
        // check for errors
        if err = scanner.Err(); err != nil {
            log.Fatal(err)
        }

    } else {
        log.Fatal(err)
    }
}

func readFeederMetadata(fileName string) map[string]int64 {
    customers := make(map[string]int64)
    if file, err := os.Open(fileName); err == nil {
        defer file.Close()
        scanner  := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            lineComponents := strings.Split(line, ",")
            numCustomers, _ := strconv.ParseInt(lineComponents[8], 10, 64)
            // fmt.Printf("feeder: %s, customers: %s\n", lineComponents[0], lineComponents[8])
            customers[lineComponents[0]] = numCustomers
        }
    } else {
        log.Fatal(err)
    }
    return customers
}
