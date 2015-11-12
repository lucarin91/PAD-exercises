#!/bin/bash
mvn clean package
hadoop fs -rmr output
hadoop jar target/wordcount-1.0.jar wordcount.WordCount data/snippets output
hadoop fs -cat output/part-r-* > res
