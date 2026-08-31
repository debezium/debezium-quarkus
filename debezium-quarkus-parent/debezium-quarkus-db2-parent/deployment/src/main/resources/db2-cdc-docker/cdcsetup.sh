#!/bin/bash

if [ ! -f /asncdctools/src/asncdc.nlk ]; then
rc=1
echo "Waiting for db2inst1 to exist ..."
while [ "$rc" -ne 0 ]
do
   sleep 5
   id $DB2INSTANCE
   rc=$?
   echo '.'
done

su  -c "/asncdctools/src/dbsetup.sh $DBNAME"   - $DB2INSTANCE
fi
touch /asncdctools/src/asncdc.nlk

# Wait for CDC infrastructure to be ready
sleep 10

# Start capture agent via HOME-relative path so that ASNCDCSERVICES pgrep pattern matches
su - $DB2INSTANCE -s /bin/bash -c "nohup ~/sqllib/bin/asncap capture_schema=asncdc capture_server=$DBNAME > /tmp/asncap.log 2>&1 &"

echo "CDC setup completed successfully"
