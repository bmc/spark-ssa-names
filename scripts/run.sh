#!/bin/sh

. `dirname $0`/_defs.sh

case $# in
  3)
    class=$1
    datadir=$2
    parquet=$3
    cores=$CORES
    ;;
  4)
    class=$1
    datadir=$2
    parquet=$3
    cores=$4
    ;;
  *)
    echo "Usage: $0 class data parquet [cores]" >&2
    exit 1
    ;;
esac

rm -rf tmp
mkdir -p tmp/logs

$SPARK_HOME/bin/spark-shell \
  --properties-file $CONF \
  --executor-memory $MEMORY \
  --class $class \
  --jars $JAR_FILE \
  $MASTER \
  $datadir \
  $parquet \
  $cores
