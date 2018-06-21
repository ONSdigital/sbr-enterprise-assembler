test data files notes:
Scripts in this directory are for populating hbase with data.
Assuming following hbase tables are created already:
1. 'ons:ENT' column family 'd'
2. 'ons:LINKS' column family 'l'
3. 'ons:LOU' column family 'd'

hbase commands to populate ENT and LINKS tables:
./hbase shell /Users/[your-user-name]/.../sbr-enterprise-assembler/src/test/resources/scripts/completeScript


in newPeriod.json:

1.PayeRef: "1777L" added to item id:100000459235 (IBM-3)
2. New LU added: id:999000508999, "BusinessName": "NEW ENTERPRISE LU"
3. item, id: 100002826247 does not have "CompanyNo": "00032261"
4. item, id: 100000508723, "CompanyNo": "04223165" changed to

