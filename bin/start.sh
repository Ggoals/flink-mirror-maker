#!/usr/bin/env bash
./bin/flink run -m myJMHost:8081 \
                       ./examples/batch/WordCount.jar \
                       ../conf/application.conf