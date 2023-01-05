#!/bin/bash -x
if [ ! -d target ]; then
    mvn -T 1C -ntp -Pcore-modules,-main clean install -DskipTests -Dlicense.skip=true -Drat.skip=true -Dcheckstyle.skip=true
fi
mvn -pl pulsar-broker -Dtest=SimpleSchemaTest test