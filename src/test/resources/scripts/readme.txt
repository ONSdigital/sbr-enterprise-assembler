test data files notes:
Scripts in this directory are for populating hbase with data.
Assuming following hbase tables are created already:
1. 'ons:ENT' column family 'd'
2. 'ons:LINKS' column family 'l'

hbase commands to populate ENT and LINKS tables:
./hbase shell /Users/[your-user-name]/.../sbr-enterprise-assembler/src/test/resources/scripts/completeScript


in newPeriod.json:

1.PayeRef: "3333L" added to IBM-3
2. New LU added: id:999000508999, "BusinessName": "NEW ENTERPRISE LU"

