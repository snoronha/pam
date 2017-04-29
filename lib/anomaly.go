package lib

import (
    "fmt"
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

func (a *Anomaly) Format() string {
    return fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%d",
        a.Id, a.Anomaly, a.DeviceId, a.DevicePhase, a.DeviceType, a.FeederId, a.Signal, a.Value, a.Time, a.EpochTime)
}

