#!/bin/bash

# on Ubuntu 18 or older:
# curl -Ls https://aka.ms/downloadazcopy-v10-linux | tar -zx ; cp ./azcopy_linux_amd64_*/azcopy ./ ; sudo apt install xmlstarlet parallel jq

AZ_VERSION="2018-03-28"
LCPL=1000 # List Containers Page Limit
CONCURRENCY=5 # Parallel azcopy execution
LOAD_LIMIT=10 # keep system load avg lower than this by executing less azcopy in parallel
#TEMP_STORAGE=/mnt/azcopy # Default to ~/.azcopy can overflow your home dir. Can be few gigs in size. Uncomment to move it.

# SOURCE:
AZ_BLOB_URL="https://xxxxxxxxxx.blob.core.windows.net"
AZ_SAS_TOKEN="sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupix&se=2021-12-07T19:13:40Z&st=2021-11-30T11:13:40Z&spr=https&sig=xxxxx%3D"

# DEST:
DEST_AZ_BLOB_URL="https://xxxxxxxx.blob.core.windows.net"
DEST_AZ_SAS_TOKEN="sv=2020-08-04&ss=b&srt=sco&sp=rwdlacx&se=2021-12-07T18:57:59Z&st=2021-11-30T10:57:59Z&spr=https&sig=xxxxxx%3D"

SELF_NAME=$0

if [ ! -z "${TEMP_STORAGE}" ]
then
  export AZCOPY_LOG_LOCATION="${TEMP_STORAGE}/logs"
  export AZCOPY_JOB_PLAN_LOCATION="${TEMP_STORAGE}/plans"

  if [ ! -w "${TEMP_STORAGE}" ]
  then
    echo "error: Can't write to: ${TEMP_STORAGE}"
    exit 1
  fi
fi

###
# One container sync
###

if [ ! -z "$1" ]
then
#  echo sync container $@
  DATE_NOW=$(date -Ru | sed 's/\+0000/GMT/')
  CONT=$( printf %s "$1" | jq -sRr @uri ) # URL ENCODED CONTAINER NAME
  FOUND_DEST_CONT=$( curl -s -X GET -H "x-ms-date: ${DATE_NOW}" -H "x-ms-version: ${AZ_VERSION}" "${DEST_AZ_BLOB_URL}/?comp=list&maxresults=1&prefix=$CONT&${DEST_AZ_SAS_TOKEN}" \
    | xmlstarlet sel -t -c "count(EnumerationResults/Containers/Container)" )
  if [ "$FOUND_DEST_CONT" == "0" ]
  then
    echo Create dest container: $1
    curl -s -X PUT -H "Content-Length: 0" -H "x-ms-date: ${DATE_NOW}" -H "x-ms-version: ${AZ_VERSION}" "${DEST_AZ_BLOB_URL}/$CONT?restype=container&${DEST_AZ_SAS_TOKEN}" 2>&1
  fi
  ./azcopy sync --log-level ERROR "${AZ_BLOB_URL}/$CONT?${AZ_SAS_TOKEN}" "${DEST_AZ_BLOB_URL}/$CONT?${DEST_AZ_SAS_TOKEN}" 2>&1
  RES=$?
  if [ $RES == "0" ]; then
    echo "$1" >> containers_synced_list.txt
  else
    echo "$1" >> containers_error_list.txt
  fi
  ###
  # EXIT
  ###
  exit $RES
fi


###
# Process errored list
# uncomment and put valid filename after -a
# file should contain container names line by line
###
#time nice parallel -v -j${CONCURRENCY} --load ${LOAD_LIMIT} -a a.txt ${SELF_NAME} {} 2>&1 | tee containers_all.log
#exit



###
# Default mode with no arguments
###

[[ -f containers_synced_list.txt ]] && mv containers_synced_list.txt containers_synced_list.txt.bkp
[[ -f containers_error_list.txt ]] && mv containers_error_list.txt containers_error_list.txt.bkp
[[ -f containers_all_list.txt ]] && mv containers_all_list.txt containers_all_list.txt.bkp
[[ -f containers_all.log ]] && mv containers_all.log containers_all.log.bkp

function fetch_containers {
  DATE_NOW=$(date -Ru | sed 's/\+0000/GMT/')
  [[ -z "${NEXT_MARKER}" ]] || MARKER_PARAM="&marker=${NEXT_MARKER}"

  RES=$(curl -s -X GET -H "x-ms-date: ${DATE_NOW}" -H "x-ms-version: ${AZ_VERSION}" "${AZ_BLOB_URL}/?comp=list&maxresults=${LCPL}${MARKER_PARAM}&${AZ_SAS_TOKEN}")

  NEXT_MARKER=$(echo $RES | xmlstarlet sel -t -v EnumerationResults/NextMarker)

  CONTAINERS_COUNT=$( echo $RES | xmlstarlet sel -t -c "count(EnumerationResults/Containers/Container)" )
  ((FETCHED_CONTS=FETCHED_CONTS+CONTAINERS_COUNT))
  echo Containers listed for sync: $FETCHED_CONTS

  echo $RES | xmlstarlet sel -t -v EnumerationResults/Containers/Container/Name >> containers_all_list.txt
  echo "" >> containers_all_list.txt
}


date
echo "star fetching containers"
FETCHED_CONTS=0
fetch_containers
until [ -z "$NEXT_MARKER" ]
do
  fetch_containers
done

date
echo "finished fetching containers"

time nice parallel -v -j${CONCURRENCY} --load ${LOAD_LIMIT} -a containers_all_list.txt ${SELF_NAME} {} 2>&1 | tee containers_all.log