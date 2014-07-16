

echo "Loading ip address"

for i in {122..255}

do
echo "Loading ip address for table ip4_$i"
/home/training/Installations/sqoop-1.4.2.bin__hadoop-1.0.0/bin/sqoop import --connect jdbc:mysql://pgrwrite.db.7322579.hostedresource.com/pgrwrite --username pgrwrite --password Sravya0828 --table ip4_$i --target-dir /ipaddress/$i --split-by b --num-mappers 8
echo "Loading ip address for table ip4_$i done"
done

echo "Loading ip address done"
