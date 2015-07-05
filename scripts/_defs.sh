#@IgnoreInspection BashAddShebang
: ${SPARK_HOME:=$HOME/local/spark}
#MASTER=spark://`hostname`:7077
MASTER='local[4]'
CORES=2
: ${JAR_FILE:=target/scala-2.10/ssn-names_2.10-1.0.jar}
MEMORY=2G

_here=`dirname $0`
_parent=`dirname $_here`
ROOT=`(cd $_parent; pwd)`

CONF=$ROOT/conf/spark-defaults.conf
