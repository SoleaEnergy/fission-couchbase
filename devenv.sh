#!/bin/sh

export GOPROXY='https://nexus.corp.soleaenergy.com/repository/nexusgo'
export GONOSUMDB='github.com/SoleaEnergy/*'
go mod tidy
go mod tidy