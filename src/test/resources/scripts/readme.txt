test data files notes:
Scripts in this directory are for populating hbase with data.
Assuming following hbase tables are created already:
1. 'ons:ENT' column family 'd'
2. 'ons:LINKS' column family 'l'
3. 'ons:LOU' column family 'd'

hbase commands to populate ENT and LINKS tables:
./hbase shell /Users/[your-user-name]/.../sbr-enterprise-assembler/src/test/resources/scripts/completeScript


in newPeriod.json:

1. "CompanyNo": "00032261" removed from item, id: "100002826247"
2. Item, id: "100000508723", "CompanyNo": "04223165" changed to "01113199"
3. Item, id: "100000827984", "CompanyNo": "00032263" changed to "04186804"
4. PAYE ref "3333L" added to item, id: "100000508724"
5. New BI item added: id: "999000508999", "BusinessName": "NEW ENTERPRISE LU", resulting in additional following changes:
   5.1 new ENT added with dynamically generated id,
   5.2 new LEU, id: 999000508999 added
   5.3 new LOU added with dynamically generated id
   5.4 "CompanyNo": "33322444" added to LEU, id: 999000508999
   5.5 VAT ref "919100010" added to LEU, id: 999000508999

Affected:
LINKS table  rows change from 38 to 43:

step 1:    -1 row
step 2:    no effect on row count
step 3:    no effect on row count
step 4:    +1 row
step 5:    n/a
step 5.1:  +1 row
step 5.2:  +1 row
step 5.3:  +1 row
step 5.4:  +1 row
step 5.5:  +1 row
--------------------
TOTAL     +5 rows



