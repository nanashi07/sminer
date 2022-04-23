#!/bin/bash -e

function get_name()
{
    echo $1 | sed -r 's/.+\.(.+)\.json/\1/g' # mac
}
export -f get_name

PATTERN=$1

# get config path
if [[ ! -e "$CONFIG_FILE" ]]
then
  CONFIG_FILE=config.yaml
fi

CONFIG_SHA=`sha1sum $CONFIG_FILE | awk '{print $1}'`
mkdir -p tmp/$CONFIG_SHA
cp -r $CONFIG_FILE tmp/$CONFIG_SHA/config.yaml
export SMINER_BASE_DIR=tmp/$CONFIG_SHA
export SMINER_CONFIG_FILE=tmp/$CONFIG_SHA/config.yaml
export RUST_BACKTRACE=full
export SMINER_BIN=./target/release/sminer
# cat $SMINER_CONFIG_FILE

./target/release/sminer annotate
echo `date` > $SMINER_BASE_DIR/start-time.txt

if [[ "$PATTERN" == "" ]];
then
  find tmp/json -type f | grep ticker | sort | xargs -n1 bash -c '$SMINER_BIN replay -f $SMINER_CONFIG_FILE $1 | tee $SMINER_BASE_DIR/replay.`get_name $1`.`date +%s`.log' _
else
  find tmp/json -type f | grep ticker | grep $PATTERN | sort | xargs -n1 bash -c '$SMINER_BIN replay -f $SMINER_CONFIG_FILE $1 | tee $SMINER_BASE_DIR/replay.`get_name $1`.`date +%s`.log' _
fi



