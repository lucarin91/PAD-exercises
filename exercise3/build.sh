#!/bin/bash
mvn clean package
hadoop fs -rmr output
hadoop jar target/exercise3-1.0.jar findbgrams.FindBGrams data/snippets output
hadoop fs -cat output/part-r-* > res
