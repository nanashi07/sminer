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
export SMINER_BASE_DIR=tmp/replay/$CONFIG_SHA
export SMINER_CONFIG_FILE=$SMINER_BASE_DIR/config.yaml
export RUST_BACKTRACE=full
export SMINER_BIN=./target/release/sminer

mkdir -p $SMINER_BASE_DIR/log
cp -r $CONFIG_FILE $SMINER_BASE_DIR/config.yaml
# cat $SMINER_CONFIG_FILE

./target/release/sminer annotate
echo `date` > $SMINER_BASE_DIR/start-time.txt

if [[ "$PATTERN" == "" ]];
then
  find tmp/json -type f | grep ticker | sort | xargs -n1 bash -c '$SMINER_BIN replay -f $SMINER_CONFIG_FILE $1 | tee $SMINER_BASE_DIR/log/replay.`get_name $1`.`date +%s`.log' _
else
  find tmp/json -type f | grep ticker | grep $PATTERN | sort | xargs -n1 bash -c '$SMINER_BIN replay -f $SMINER_CONFIG_FILE $1 | tee $SMINER_BASE_DIR/log/replay.`get_name $1`.`date +%s`.log' _
fi

# generate report
echo "| Symbols    | Date       | Order count | Loss orders | Loss orders (%s) | Total amount | PnL          | PnL (%s)   | Config SHA                               |" >  $SMINER_BASE_DIR/report.md
echo "|------------|------------|-------------|-------------|------------------|--------------|--------------|------------|------------------------------------------|" >> $SMINER_BASE_DIR/report.md
grep -R -h $CONFIG_SHA $SMINER_BASE_DIR/log/*.log | sort | cut -c 28- >> $SMINER_BASE_DIR/report.md

# get all symbols
echo '# Summary' >  $SMINER_BASE_DIR/summary.md
echo ""          >> $SMINER_BASE_DIR/summary.md

for PAIR in `grep $CONFIG_SHA $SMINER_BASE_DIR/report.md | awk '{print $2}' | sort | uniq`
do
  echo '##' $PAIR >> $SMINER_BASE_DIR/summary.md
  grep $CONFIG_SHA $SMINER_BASE_DIR/report.md | grep $PAIR | sort | awk '{print $6"\t"$12"\t"$14"\t"$16}'  >> $SMINER_BASE_DIR/summary.md
  echo ""          >> $SMINER_BASE_DIR/summary.md
  echo ""          >> $SMINER_BASE_DIR/summary.md
done

# optional, get date list
#grep SPX $SMINER_BASE_DIR/report.md | awk '{print $4}' 