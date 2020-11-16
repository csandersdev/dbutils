#!/bin/bash

DIRNAME=`dirname $0`
SCRIPTDIR=`readlink -f $DIRNAME`

. ${SCRIPTDIR}/env.cfg

java -classpath ${CLASSPATH} org.sandersc.util.db.client.ExecSQL "$@"