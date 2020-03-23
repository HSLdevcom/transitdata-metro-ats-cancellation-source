#!/bin/bash

if [[ "${DEBUG_ENABLED}" = true ]]; then
  java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -jar /usr/app/transitdata-metro-ats-cancellation-source.jar
else
  java -jar /usr/app/transitdata-metro-ats-cancellation-source.jar
fi
