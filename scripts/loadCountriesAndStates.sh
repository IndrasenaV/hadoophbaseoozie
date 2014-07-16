echo "Loading countries"

/home/training/Installations/sqoop-1.4.2.bin__hadoop-1.0.0/bin/sqoop import --connect jdbc:mysql://pgrwrite.db.7322579.hostedresource.com/pgrwrite --username pgrwrite --password Sravya0828 --table countries --target-dir /countries --num-mappers 1

echo "Loading Countries Done"
echo "Loading cities by countries"
/home/training/Installations/sqoop-1.4.2.bin__hadoop-1.0.0/bin/sqoop import --connect jdbc:mysql://pgrwrite.db.7322579.hostedresource.com/pgrwrite --username pgrwrite --password Sravya0828 --table cityByCountry --target-dir /cities --num-mappers 1

echo "Loading cities by countries done"
