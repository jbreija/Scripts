#!/bin/bash
# DESCRIPTION: UNIX Daemon for copying contents of hard drives upon insertion then erasure

# setup log file
exec &> >(tee -a /opt/app/log/auto-mount.log)

# email alerts and notifications
TEST_EMAILS='your.name@domain.com'

# erase data on disk after successful processing
function erase_data()
{
    # $1 $2 $3 are positional arguments
    echo "$(date) Unmounting drive on bay $3 at $2"
    if sudo umount $2 ; then
        #sudo rm -fr $2/*
        #sudo rmdir $2
        if [ ! -e $2 ]; then # if the mount point folder does not exist anymore
            echo "Data erasure for drive in bay $3 is successful"
        else
            echo "Data erasure at $1 $2 for drive in bay $3 is not successful" | mailx -r auto_mount -s "Alert - Data erasure failed" $TEST_EMAILS
        fi

    else
        echo "Drive $1 at $2 in bay $3 failed to unmount" | mailx -r auto_mount -s "Alert - Hard drive failed to unmount" $TEST_EMAILS
    fi
}

### Step 0. DAEMON BEGIN - FILE LISTENER
DETECTDISK="/dev/disk/by-path"
inotifywait -m -e create --format '%w%f' "${DETECTDISK}" | while read NEWDISK
do
    if [[ $NEWDISK == *"-sas-"* ]] && [[ $NEWDISK == *"-part"* ]]; then

        ### Step 1. Logging info
        DRIVE_BAY_NUM=$(echo "$NEWDISK" | grep -o 'phy[0-9]*-' | grep -o [0-9]*)
        DEVICE_NAME=$(readlink -f $NEWDISK)
        MOUNTDIR=$(echo "/media/"$(basename $NEWDISK))
        log="$NEWDISK $MOUNTDIR $DRIVE_BAY_NUM "
        echo "$(date) $log Found new hard drive device $DEVICE_NAME. Mounting to $MOUNTDIR now..."

        ### Step 1. Verify the partition type is ext or ntfs
        FILE_SYSTEM=$(sudo /sbin/blkid $NEWDISK -o value -s TYPE)
        if [[ $FILE_SYSTEM == *"ntfs"* ]] || [[ $FILE_SYSTEM == *"ext"* ]]; then
             
            ### Step 2. mount hard drive to filesystem
            sudo mkdir $MOUNTDIR
            sudo mount $NEWDISK $MOUNTDIR
            sudo chown -R group:users $MOUNTDIR
            if [ -d $MOUNTDIR ]; then

                ### Step 3. log hard drive stats
                DRIVE_BAY_NUM=$(echo "$NEWDISK" | grep -o 'phy[0-9]*-' | grep -o [0-9]*)
                DEVICE_NAME=$(readlink -f $NEWDISK)
                MOUNTDIR=$(echo "/media/"$(basename $NEWDISK))
                DRIVE_SPECS=$(sudo hdparm -I $NEWDISK)
                MODEL_NUMBER=$(echo "$DRIVE_SPECS" | echo $(awk '/Model Number:/{ print $0 }'))
                MODEL_NUMBER="${MODEL_NUMBER/Model Number:/}"
                SERIAL_NUMBER=$(echo "$DRIVE_SPECS" | echo $(awk '/Serial Number:/{ print $0 }'))
                SERIAL_NUMBER="${SERIAL_NUMBER/Serial Number:/}"
                TOTAL_SPACE=$(df -h $NEWDISK | sed -n 2p | awk '{print $2}')
                FREE_SPACE=$(df -h $NEWDISK | sed -n 2p | awk '{print $4}')
                USED_SPACE=$(df -h $NEWDISK | sed -n 2p | awk '{print $3}')
                STATS=$(date $log $MODEL_NUMBER $SERIAL_NUMBER $TOTAL_SPACE $FREE_SPACE $USED_SPACE)
                
                echo "$(date) $log Verified new drive mounted at $MOUNTDIR is writable. Invoking processor."

				### Step 4. email notification to inform that processing is about to start
                echo $STATS | mail -s "Data Notification - New data ingestion commencing" $TEST_EMAILS

                ### Step 5. invoke processor
                /usr/bin/python2.7 /opt/app/processor/processor.py -i $MOUNTDIR metadata -s $SERIAL_NUMBER -d $DRIVE_BAY_NUM
                RESULTS=$?
                RESULTS_LOG=$(find /opt/app/processor/log -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -f2- -d" ")
                LOG_FILE=`basename $RESULTS_LOG`
                echo "$(date) $log Bag processor result code is $RESULTS (0 successful, 1 fail)"

                ### Step 6. email results
                echo "value of Results_Log is $RESULTS_LOG and value of Log file is $LOG_FILE"
                (echo "See attached for processor log"; uuencode $RESULTS_LOG $LOG_FILE) | mailx -r auto_mount -s "Data Notification - Results" $TEST_EMAILS

                ### Step 7. if results were successful, erase and unmount the drives
                if [ "$RESULTS" == 0 ]; then

                    ### Step 8. make sure no open files first
                    sleep 1
                    cmd=$(lsof -t +d "$MOUNTDIR")
                    if [ -z $cmd ]; then
                        echo "$(date) $log Verified no files open on $MOUNTDIR"
                        erase_data "$log"
                    else
                        echo -e "$(date) $log Found open processes on $MOUNTDIR\n$cmd\nKilling them now"
                        lsof +d $MOUNTDIR
                        kill -9 $cmd
                        erase_data "$log"

                    ### Step 9. email for hard drive removal
                    
                    echo "Hard drive in slot number $DRIVE_BAY_NUM with serial# $SERIAL_NUMBER has finished processing and can be removed." | mailx -r auto_mount -s "Hard Drive Notification - Drive ready for removal" $TEST_EMAILS
                    ### - END INGESTION
                    fi

                else # FAIL
                    echo "Error: failed to mount\n$STATS" | mailx -r auto_mount -s "Alert - Drive failed to mount" $TEST_EMAILS
                fi
            fi
        else # LOGGING
            echo "Found non ext and ntfs file system $FILE_SYSTEM on bay $DRIVE_BAY_NUM device $DEVICE_NAME"
        fi
    fi
done
