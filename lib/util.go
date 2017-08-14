package lib

// Sort arrays of int64
type int64arr []int64

func (a int64arr) Len() int {
    return len(a)
}

func (a int64arr) Swap(i, j int) {
    a[i], a[j] = a[j], a[i]
}

func (a int64arr) Less(i, j int) bool {
    return a[i] < a[j]
}


// Create a y object
type YObject struct {
    Feeder    string
    Timestamp int64
    Outage    int64
    Ticket    string
}
