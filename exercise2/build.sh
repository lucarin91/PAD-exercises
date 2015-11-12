#!/bin/bash
mvn clean package
hadoop fs -put ../data/pg100.txt pg100.txt
hadoop fs -rmr output
hadoop jar target/exercise2-1.0.jar allcaps.AllCaps data/snippets output
hadoop fs -cat output/part-r-* > res
