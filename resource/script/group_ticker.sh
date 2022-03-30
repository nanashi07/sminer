#!/bin/bash -e

SYMBOLS='TQQQ SQQQ SOXL SOXS SPXL SPXS LABU LABD TNA TZA YINN YANG UDOW SDOW'
FILES=$@

for SYMBOL in $SYMBOLS
do
  for FILE in $FILES
  do
    if [[ -f "$FILE" ]];
    then
      echo splitting $SYMBOL from $FILE
      grep $SYMBOL $FILE > split.$FILE.$SYMBOL.json
    fi
  done
done