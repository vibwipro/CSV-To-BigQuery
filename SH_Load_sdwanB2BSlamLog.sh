#!/bin/ksh

#**************************************************************************
#Script Name    : SH_Load_alarmLog_1Day.sh
#Description    : This script will collect last 1 day records and that will considered for loading BQ-table
#Created by     : Vibhor Gupta
#Version        Author          Created Date    Comments
#1.0            Vibhor          2020-05-08      Initial version
#***************************************************************************

#****************************************************************
#------Parameter description--------#

# 1- File Name
# 2- Input File Path

#-----------------------------------#
#------Global variable--------------#
NO_OF_PARAMETERS=2

#-----------------------------------#
#----- Check for the number of input parameters-------#

if [ $# -ne ${NO_OF_PARAMETERS} ] || [ ${1} != "sdwanB2BSlamLog" ]
then
        echo "Error: The script expects ${NO_OF_PARAMETERS} parameters but the actual parameters that are passed are $# and file need to be processed is alarmLog"
        echo "Listed below are the list of parameters required by the script"
        echo "1- File Name which need to be processed that is alarmLog"
        echo "2- Input File Path which need to be processed"
        exit 1
fi

#-----------------------------------------------------#
#------Assigning the parametrs to variables-----------#

FILE_NAME=${1}
DATA_PATH=${2}
ARCHIVE=/mnt/disks/disk2/logs/network/Archive

#------Get the file name which need to be processed-------#
DATE=`date +%Y%m%d%H%M`
#DATE_PRO_ID=`date +%Y%m`
FILE_PROCESS="${FILE_NAME}"
JOB_NAME="${FILE_NAME,,}-${DATE}"
LIST_FILE="sdwanB2BSlamLog_${DATE}.list"
FILE_CONCAT="sdwanB2BSlamLog_${DATE}"
PRO_ID="sdwanB2BSlamLog"
ARCHIVE=/mnt/disks/disk2/logs/network/Archive
CODE_PATH=/home/syslogadmin/aiops/networking/sdwanB2BSlamLog
GCS_PATH="gs://prod-de-pipeline/networking/sdwanb2bslamlog"

ls $DATA_PATH/sdwanB2BSlamLog_*.log >> $LIST_FILE

if [ -s $LIST_FILE ]
then

    cat $LIST_FILE | while read line
    do
        DATA_LINE=$( echo "$line" | cut -c 47-59 )
        DATE_LINE="${DATA_LINE//[_]/}"
        if [ $DATE_LINE -lt $DATE ]
        then
            cat $line >> $FILE_CONCAT
            gzip $line
            mv "${line}.gz" $ARCHIVE
        fi
    done    


    sudo gsutil cp $FILE_CONCAT $GCS_PATH/$FILE_CONCAT

    sudo python $CODE_PATH/BQ_Load_sdwanB2BSlamLog.py  --input $GCS_PATH/$FILE_CONCAT --region europe-west1 --project PROJ --temp_location gs://prod-de-pipeline/temp --runner DataflowRunner --job_name $JOB_NAME --pro_id $PRO_ID

    if [ $? -eq 0 ]
    then

        sudo gsutil rm $GCS_PATH/$FILE_CONCAT
        rm -f $FILE_CONCAT
        rm -f $LIST_FILE
    else
        mv $FILE_CONCAT $DATA_PATH/"${FILE_CONCAT}.log"
        rm -f $LIST_FILE
    fi
else
    rm -f $LIST_FILE
    python /home/vibhg/MS_TeamsNotification.py "We are not getting any data for sdwanB2BSlamLog"
    exit 1
fi

#------ Archive Source File ----------------------------------#


echo '*************************'

echo 'Finish'


