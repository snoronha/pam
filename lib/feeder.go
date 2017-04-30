package lib

import (
    "bufio"
    _ "fmt"
    _ "io/ioutil"
    "log"
    "os"
    "strconv"
    "strings"
)

type Feeder struct {
    // FEEDER,FEEDER,INSTALL_DATE,SUBSTATION,COUNTY,KV,AREA,TYPE,CUSTOMERS,RESIDENTIAL,COMMERCIAL,INDUSTRIAL,AFS,CAPBANK,3PH_OCR,1PH_OCR,FDR_OH,FDR_UG,LAT_OH,LAT_UG,POLE_COUNT,REGION,RELAY,AFS_SCHEME,HARDENING,FI,HAS_AFS,HAS_AFS_OCR,IS_DADE,HAS_INDUSTRIAL,LENGTH,PCT_UG,OH_FDR,UG_FDR,HYBRID,CEMM35_FEEDER,4N+_FEEDER
    FeederId      string
    InstallDate   string
    KV            string
    Customers     int64
    Residential   int64
    Commercial    int64
    Industrial    int64
    FdrOh         float64
    FdrUg         float64
}

func (f *Feeder) Create(feederLine string) {
    lineComponents  := strings.Split(feederLine, ",")
    f.FeederId       = lineComponents[1]
    f.KV             = lineComponents[5]
    f.Customers, _   = strconv.ParseInt(lineComponents[8], 10, 64)
    f.Residential, _ = strconv.ParseInt(lineComponents[9], 10, 64)
    f.Commercial, _  = strconv.ParseInt(lineComponents[10], 10, 64)
    f.Industrial, _  = strconv.ParseInt(lineComponents[11], 10, 64)
    f.FdrOh, _       = strconv.ParseFloat(lineComponents[16], 64)
    f.FdrUg, _       = strconv.ParseFloat(lineComponents[17], 64)
}

func GetFeederMap(fileName string) map[string]Feeder {
    var feederMap map[string]Feeder = make(map[string]Feeder)
    if file, err := os.Open(fileName); err == nil {
        defer file.Close()
        scanner   := bufio.NewScanner(file)
        lineCount := 0
        for scanner.Scan() {
            line := scanner.Text()
            if lineCount > 0 {
                if len(strings.Split(line, ",")) >= 10 {
                    feederObj := new(Feeder)
                    feederObj.Create(line)
                    feederMap[feederObj.FeederId] = *feederObj
                }
            }
            lineCount++
        }
        if err = scanner.Err(); err != nil {
            log.Fatal(err)
        }
    } else {
        log.Fatal(err)
    }
    return feederMap
}
