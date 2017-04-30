package lib

import (
    "bufio"
    "log"
    "os"
    "strings"
    "strconv"
)

type DatasetObject struct {
    Name    string
    Lookup  string
    Type    string
    MinLag  int64
    MaxLag  int64
    KeepAll bool
}

func (d *DatasetObject) Create(datasetLine string) {
    lineComponents  := strings.Split(datasetLine, ",")
    d.Name           = lineComponents[0]
    d.Lookup         = lineComponents[1]
    d.Type           = lineComponents[2]
    d.MinLag, _      = strconv.ParseInt(lineComponents[3], 10, 64)
    d.MaxLag, _      = strconv.ParseInt(lineComponents[4], 10, 64)
    d.KeepAll, _     = strconv.ParseBool(lineComponents[5])
}

func GetDatasetMap(fileName string) map[string]DatasetObject {
    var datasetMap map[string]DatasetObject = make(map[string]DatasetObject)
    if file, err := os.Open(fileName); err == nil {
        defer file.Close()
        scanner   := bufio.NewScanner(file)
        lineCount := 0
        for scanner.Scan() {
            line := scanner.Text()
            if lineCount > 0 {
                if len(strings.Split(line, ",")) >= 5 {
                    datasetObj := new(DatasetObject)
                    datasetObj.Create(line)
                    datasetMap[datasetObj.Name] = *datasetObj
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
    return datasetMap
}
