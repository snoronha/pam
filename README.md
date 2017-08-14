## Synopsis

Anomaly processing for FPL (currently) written in Go.

## Code Structure

```
**pam**
│   **README.md** (this file)
│
└───**anomaly**
│   │   **main.go** (anomaly processing with command-line args)
│   │
└───lib
│   │   **ami.go** (AMI record structure)
│   │   **anomaly.go** (Anomaly structure with utilities)
│   │   **anomaly_map.go** (maps from computed anomal names to final anomaly names for 3 models)
│   │   **compare.go** (utilities for comparing Python anomalies with Go anomalies)
│   │   **dataset.go** (structure to encapsulate different datasets)
│   │   **edna.go** (EDNA record structure)
│   │   **feeder.go** (Feeder record structure)
│   │   **process_ami.go** (process AMI anomalies)
│   │   **process_edna.go** (process EDNA anomalies)
│   │   **process_scada.go** (process SCADA anomalies)
│   │   **process_signature.go** (process signatures)
│   │   **s3.go** (utilities to read/write S3 buckets for monthly data)
│   │   **ticket.go** (Ticket record structure)
│   │   **util.go** (utils for signature processing)
│   │   **window.go** (moving time-window implementation)
│   │
└───output
│   │   ... (output files written here)
│   │
```

## Motivation

Anomaly processing for FPL (currently) written in Go. The Python code had serious performance issues (mostly related to timestamp processing). This implementation fixes that problem.

## Install/Compile

* [Install Go][https://golang.org/doc/install] (version 1.8+) on your system
* Create a Go home directory (e.g. ~/go)
* Export **GOPATH** (e.g. `export GOPATH="/Users/sanjaynoronha/go"`)
* cd $GOPATH
* mkdir src; mkdir bin; mkdir pkg
* cd src
* git clone https://github.com/snoronha/pam
* cd pam/anomaly
* go install # this will install a binary `anomaly` in $GOPATH/bin

## Tests

Tests here

## License

A short snippet describing the license (MIT, Apache, etc.)