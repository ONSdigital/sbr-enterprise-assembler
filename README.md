# Enterprise Assembler

Created new and update existing entities.

## Application Arguments

The following details input parameters with the required options (*This list should probably be extended*)  being:

```

quorum - The list of HBase Zookeeper hosts
port   - The port number for the above
seq    - The HOST:PORT for the Zookeeper instance(s) used for unique sequence number generation 

```

General usage is as follows:

```
usage: sbr-enterprise-assembler [-bi <FILE PATH>] [-d] [-e <arg>] [-ecf <COLUMN FAMILY>] [-efp <FILE
       PATH>] [-ens <NAMESPACE>] [-etn <TABLE NAME>] [-geo <FILE PATH>] [-geoShort <FILE PATH>]
       [-hiveDB <DATABASE NAME>] [-hiveShortTable <TABLE NAME>] [-hiveTable <TABLE NAME>] [-lcf
       <COLUMN FAMILY>] [-lecf <COLUMN FAMILY>] [-lefp <FILE PATH>] [-lens <NAMESPACE>] [-letn
       <TABLE NAME>] [-lfp <FILE PATH>] [-lns <NAMESPACE>] [-locf <COLUMN FAMILY>] [-lofp <FILE
       PATH>] [-lons <NAMESPACE>] [-lotn <TABLE NAME>] [-ltn <TABLE NAME>] [-paye <FILE PATH>] -port
       <PORT> -quorum <HOST[,HOST...]> [-recf <COLUMN FAMILY>] [-refp <FILE PATH>] [-rens
       <NAMESPACE>] [-retn <TABLE NAME>] -seq <HOST:PORT[,HOST:PORT...]> [-tp <TIME PERIOD>] [-vat
       <FILE PATH>]

(Re)Run the SBR Enterprise Assembler

 -bi,--bi-file-path <FILE PATH>                         the BI parquet input file path
 -d,--debug                                             switch debugging on
 -e,--environment <arg>                                 local | cluster
 -ecf,--enterprise-column-family <COLUMN FAMILY>        enterprise column family
 -efp,--enterprise-file-path <FILE PATH>                enterprise file path
 -ens,--enterprise-table-namespace <NAMESPACE>          enterprise table namespace
 -etn,--enterprise-table-name <TABLE NAME>              HBase Enterprise table name
 -geo,--path-to-geo <FILE PATH>                         GEO file path
 -geoShort,--path-to-geo-short <FILE PATH>              GEO short file path
 -hiveDB,--hive-db-name <DATABASE NAME>                 Hive database name
 -hiveShortTable,--hive-short-table-name <TABLE NAME>   Hive short table name
 -hiveTable,--hive-table-name <TABLE NAME>              Hive table name
 -lcf,--links-column-family <COLUMN FAMILY>             links column family
 -lecf,--legal-column-family <COLUMN FAMILY>            legal column family
 -lefp,--legal-file-path <FILE PATH>                    legal file path
 -lens,--legal-table-namespace <NAMESPACE>              legal table namespace
 -letn,--legal-table-name <TABLE NAME>                  HBase legal table name
 -lfp,--links-file-path <FILE PATH>                     links file path
 -lns,--links-table-namespace <NAMESPACE>               links table namespace
 -locf,--local-column-family <COLUMN FAMILY>            local table column family
 -lofp,--local-file-path <FILE PATH>                    local file path
 -lons,--local-table-namespace <NAMESPACE>              local table namespace
 -lotn,--local-table-name <TABLE NAME>                  HBase local table name
 -ltn,--links-table-name <TABLE NAME>                   HBase links table name
 -paye,--paye-file-path <FILE PATH>                     PAYE file path
 -port,--zookeeper-port <PORT>                          port for the HBase zookeeper instance(s)
 -quorum,--zookeeper-quorum <HOST[,HOST...]>            host[,host...] for the HBase zookeeper
                                                        instance(s)
 -recf,--reporting-column-family <COLUMN FAMILY>        reporting table column family
 -refp,--reporting-file-path <FILE PATH>                reporting file path
 -rens,--reporting-table-namespace <NAMESPACE>          reporting table namespace
 -retn,--reporting-table-name <TABLE NAME>              HBase reporting table name
 -seq,--seq-url <HOST:PORT[,HOST:PORT...]>              a list of HOST:PORT[,HOST:PORT...] for the
                                                        Zookeeper sequence number generator hosts(s)
 -tp,--time-period <TIME PERIOD>                        time period
 -vat,--vat-file-path <FILE PATH>                       VAT file path
```

## Units HBase Table Schemas
```

                                ENTERPRISE
    +--------------------+--------------------+----------------------------------------------------------------------------------------------------------------------------+
    |   column name      |     Optionality    |     Description                                                                                                            |
    +--------------------+--------------------+----------------------------------------------------------------------------------------------------------------------------+
    |    ern             |    Mandatory       |  Unique 10 digit enterprise reference generated for SBR                                                                    |
    |    prn             |    Mandatory       |  PRN value calculated at time of unit creation and fixed for the life of the unit                                          |
    |    entref          |    Optional        |  Unique 10 digit enterprise reference from IDBR                                                                            |
    |    name            |    Mandatory       |  Name for the enterprise                                                                                                   |
    |    trading_style   |    Optional        |  Trading as name/ Alternative name                                                                                         |
    |    address1        |    Optional        |                                                                                                                            |
    |    address2        |    Optional        |                                                                                                                            |
    |    address3        |    Optional        |                                                                                                                            |                                                                                      
    |    address4        |    Optional        |                                                                                                                            |
    |    address5        |    Optional        |                                                                                                                            |
    |    postcode        |    Mandatory       |                                                                                                                            |
    |    region          |    Mandatory       |  Geographic region allocated through a geography lookup from Postcode (defined as GOR in source file). 9 character string  |                                                                                           |
    |    sic07           |    Mandatory       |  Standard Industrial Classification (SIC)                                                                                  |
    |    legal_status    |    Mandatory       |                                                                                                                            |
    |    paye_empees     |    Optional        |  average of PAYE jobs across year                                                                                          |
    |    paye_jobs       |    Optional        |  Sum of PAYE jobs for latest period                                                                                        |
    |    cntd_turnover   |    Optional        |  Sum of Rep VAT turnover for contained rep VAT Groups                                                                      |
    |    app_turnover    |    Optional        |  Apportioned Rep VAT turnover based on employees                                                                           |
    |    std_turnover    |    Optional        |  Sum of turnover for standard vats                                                                                         |
    |    grp_turnover    |    Optional        |  Sum of Rep VAT group turnover                                                                                             |                  
    |    ent_turnover    |    Optional        |  Sum of all (standard + contained + apportioned) turnover values for that enterprise                                       |
    |    working_props   |    Mandatory       |  Calculated from Legal Status (Sole Proprietors = 1; Partnerships & LLP = 2; all others = 0).                              |
    |    employment      |    Mandatory       |  Sum of Employees (admin) and Working Proprietors                                                                          |
    |                    |                    |                                                                                                                            |
    =--------------------+--------------------+----------------------------------------------------------------------------------------------------------------------------+
    
                                    LOCAL UNIT
    +--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
    |   column name      |     Optionality    |     Description                                                                                                             |
    |--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
    |    lurn            |    Mandatory       |  Unique 9 digit ref                                                                                                         |
    |    luref           |    Optional        |  Unique 9 digit ref                                                                                                         |
    |    ern             |    Mandatory       |  Unique 10 digit enterprise reference generated for SBR                                                                     |
    |    prn             |    Mandatory       |  PRN value calculated at time of unit creation and fixed for the life of the unit                                           |
    |    rurn            |    Mandatory       |  Unique 11 digit ref                                                                                                        |
    |    ruref           |    Optional        |  Unique 11 digit ref                                                                                                        |
    |    name            |    Mandatory       |                                                                                                                             |
    |    entref          |    Optional        |  Unique 10 digit enterprise reference from IDBR                                                                             |                                                                                      
    |    trading_style   |    Optional        |                                                                                                                             |
    |    address1        |    Mandatory       |                                                                                                                             |
    |    address2        |    Optional        |                                                                                                                             |
    |    address3        |    Optional        |                                                                                                                             |
    |    address4        |    Optional        |                                                                                                                             |
    |    address5        |    Optional        |                                                                                                                             |
    |    postcode        |    Mandatory       |                                                                                                                             |
    |    region          |    Mandatory       |  Geographic region allocated through a geography lookup from Postcode (defined as GOR in source file).9 character string    |
    |    sic07           |    Mandatory       |  Standard Industrial Classification (SIC)                                                                                   |
    |    employees       |    Mandatory       |                                                                                                                             |                
    |    employment      |    Mandatory       |                                                                                                                             |
    |--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
    
                                        REPORTING UNIT
    +--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
    |   column name      |     Optionality    |     Description                                                                                                             |
    |--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
    |    rurn            |    Mandatory       |                                                                                                                             |
    |    ruref           |    Optional        |                                                                                                                             |
    |    ern             |    Mandatory       |                                                                                                                             |
    |    prn             |    Mandatory       |                                                                                                                             |
    |    name            |    Mandatory       |                                                                                                                             |
    |    entref          |    Optional        |                                                                                                                             |                                                                                      
    |    trading_style   |    Optional        |                                                                                                                             |
    |    legal_status    |    Optional        |                                                                                                                             |
    |    address1        |    Mandatory       |                                                                                                                             |
    |    address2        |    Optional        |                                                                                                                             |
    |    address3        |    Optional        |                                                                                                                             |
    |    address4        |    Optional        |                                                                                                                             |
    |    address5        |    Optional        |                                                                                                                             |
    |    postcode        |    Mandatory       |                                                                                                                             |
    |    region          |    Mandatory       |  Geographic region allocated through a geography lookup from Postcode (defined as GOR in source file).9 character string    |
    |    sic07           |    Mandatory       |  Standard Industrial Classification (SIC)                                                                                   |
    |    employees       |    Mandatory       |                                                                                                                             |                
    |    employment      |    Mandatory       |                                                                                                                             |
    |    turnover        |    Mandatory       |                                                                                                                             |
    |--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
    
        
                                        LEGAL UNIT
    +--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
    |   column name      |     Optionality    |     Description                                                                                                             |
    |--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
    |    ubrn            |    Mandatory       |  Unique 16 digit legal unit reference                                                                                       |
    |    crn             |    Optional        |  Unique Companies House reference number                                                                                    |
    |    uprn            |    Optional        |  11 digit key allocated based on address matching to AI                                                                     |
    |    name            |    Mandatory       |  Name for the Legal unit                                                                                                    |                                                                                      
    |    trading_style   |    Optional        |                                                                                                                             |
    |    address1        |    Mandatory       |                                                                                                                             |
    |    address2        |    Optional        |                                                                                                                             |
    |    address3        |    Optional        |                                                                                                                             |
    |    address4        |    Optional        |                                                                                                                             |
    |    address5        |    Optional        |                                                                                                                             |
    |    postcode        |    Mandatory       |                                                                                                                             |
    |    sic07           |    Mandatory       |  Standard Industrial Classification (SIC)                                                                                   |
    |    legal_status    |    Mandatory       |                                                                                                                             |                
    |    trading_status  |    Optional        |  Text string consisting of A (Active), C (Closed), D (Dormant) or I (Insolvent)                                             |                
    |    birth_date      |    Mandatory       |  Formatted as dd/mm/yyyy                                                                                                    |
    |    death_date      |    Optional        |  Formatted as dd/mm/yyyy                                                                                                    |
    |    death_code      |    Optional        |                                                                                                                             |
    |--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+
```
    