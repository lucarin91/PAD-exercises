#!/bin/bash
mvn clean package
hadoop fs -rmr output
hadoop jar target/exercise4-1.0.jar brownfox.BrownFox data/pg100.txt output
hadoop fs -cat output/part-r-* > res
