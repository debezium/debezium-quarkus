#!/bin/bash

if [ ! -f /asncdctools/src/asncdc.nlk ]; then
rc=1
echo "Waiting for db2inst1 to exist ..."
while [ "$rc" -ne 0 ]
do
   sleep 5
   id db2inst1
   rc=$?
   echo '.'
done

su  -c "/asncdctools/src/dbsetup.sh $DBNAME"   - db2inst1
fi
touch /asncdctools/src/asncdc.nlk

# Wait for CDC infrastructure to be ready
sleep 10

# Run test table setup if in test mode
if [ "$TEST_MODE" = "true" ]; then
  echo "Setting up test tables..."
  su - $DB2INSTANCE -s /bin/bash -c "db2 -tvmf /asncdctools/src/testsetup.sql"
fi

# Start capture agent AFTER tables have CDC enabled
su - $DB2INSTANCE -s /bin/bash -c "nohup /database/config/$DB2INSTANCE/sqllib/bin/asncap capture_schema=asncdc capture_server=$DBNAME > /tmp/asncap.log 2>&1 &"

echo "done"
