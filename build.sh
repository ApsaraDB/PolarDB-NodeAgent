#!/bin/bash

go mod tidy
go mod vendor
make clean && make all
