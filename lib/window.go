package lib

import (
    "time"
)

// Window implementation
type Window struct {
    StartPointer int
    EndPointer int
    ts [1000]time.Time
    extendedId [1000]string
    value [1000]float64
    MAXSIZE int
}

func (w *Window) AddElement(ts time.Time, extendedId string, value float64) {
    w.EndPointer          = (w.EndPointer + 1) % w.MAXSIZE
    w.ts[w.EndPointer]    = ts
    w.extendedId[w.EndPointer] = extendedId
    w.value[w.EndPointer] = value
}

func (w *Window) Mean() float64 {
    numElems := 0
    sum      := 0.0
    start    := w.StartPointer
    end      := w.EndPointer
    if w.StartPointer > w.EndPointer {
        end = w.EndPointer + w.MAXSIZE
    }
    for i := start; i <= end; i++ {
        sum += w.value[i % w.MAXSIZE]
        numElems++
    }
    return sum/float64(numElems)
}

func (w *Window) QuantileGreaterThanThreshold(quantile float64, threshold float64) bool {
    numElems     := 0
    greaterElems := 0
    start        := w.StartPointer
    end          := w.EndPointer
    if w.StartPointer > w.EndPointer {
        end = w.EndPointer + w.MAXSIZE
    }
    for i := start; i <= end; i++ {
        if w.value[i % w.MAXSIZE] >= threshold {
            greaterElems++
        }
        numElems++
    }
    return float64(greaterElems)/float64(numElems) >= quantile
}

func (w *Window) GreaterThanThreshold(elementIndex int, threshold float64) bool {
    if elementIndex < 0 {
        elementIndex += w.MAXSIZE
    }
    elementIndex %= w.MAXSIZE
    if w.ts[elementIndex].IsZero() {
        return false
    }
    return w.value[elementIndex] >= threshold 
}

func (w *Window) LessThanThreshold(elementIndex int, threshold float64) bool {
    if elementIndex < 0 {
        elementIndex += w.MAXSIZE
    }
    elementIndex %= w.MAXSIZE
    if w.ts[elementIndex].IsZero() {
        return false
    }
    return w.value[elementIndex] <= threshold 
}

func (w *Window) SetStartPointer() {
    endTime   := w.ts[w.EndPointer]
    startTime := w.ts[w.StartPointer]
    done      := false
    for !done {
        if endTime.IsZero() || startTime.IsZero() {
            done = true
            continue
        }
        timeDiff := endTime.Sub(startTime)
        if timeDiff.Seconds() > 86400 {
            nextPointer := (w.StartPointer + 1) % w.MAXSIZE
            nextTs      := w.ts[nextPointer]
            if nextTs.IsZero() {
                done = true
                continue
            }
            if endTime.Sub(nextTs) <= 86400 {
                done = true
            } else {
                w.StartPointer = nextPointer
                startTime      = w.ts[w.StartPointer]
            }
        } else {
            prevPointer := (w.StartPointer - 1) % w.MAXSIZE
            if prevPointer < 0 {
                prevPointer += w.MAXSIZE
            }
            prevTs      := w.ts[prevPointer]
            if prevTs.IsZero() {
                done = true
                continue
            }
            if endTime.Sub(prevTs) > 86400 {
                done = true
            } else {
                w.StartPointer = prevPointer
                startTime      = w.ts[w.StartPointer]
            }
        }
        if w.StartPointer == w.EndPointer {
            done = true
        }
    }
}
