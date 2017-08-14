## Synopsis

Anomaly processing for FPL (currently) written in Go.

## Code Structure

```
__pam__
│   __README.md__ (this file)
│
└───__anomaly__
│   │   __main.go__ (anomaly processing with command-line args)
│   │
└───lib
│   │   __ami.go__ (AMI record structure)
│   │   __anomaly.go__ (Anomaly structure with utilities)
│   │   __anomaly_map.go__ (maps from computed anomal names to final anomaly names for 3 models)
│   │   __compare.go__ (utilities for comparing Python anomalies with Go anomalies)
│   │   __dataset.go__ (structure to encapsulate different datasets)
│   │   __edna.go__ (EDNA record structure)
│   │   __feeder.go__ (Feeder record structure)
│   │   __process_ami.go__ (process AMI anomalies)
│   │   __process_edna.go__ (process EDNA anomalies)
│   │   __process_scada.go__ (process SCADA anomalies)
│   │   __process_signature.go__ (process signatures)
│   │   __s3.go__ (utilities to read/write S3 buckets for monthly data)
│   │   __ticket.go__ (Ticket record structure)
│   │   __util.go__ (utils for signature processing)
│   │   __window.go__ (moving time-window implementation)
│   │
└───output
│   │   ... (output files written here)
│   │
```

## Motivation

Anomaly processing for FPL (currently) written in Go. The Python code had serious performance issues (mostly related to timestamp processing). This implementation fixes that problem.

## Install/Compile

* [[Install Go][https://golang.org/doc/install]] (version 1.8+) on your system
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