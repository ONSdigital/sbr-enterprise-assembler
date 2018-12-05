# enterprise-assemble

Created new and updates existing entities.

## Input Args:
                      HBASE LINKS TABLE NAME - name of the HBase table with units linking records,
                      HBASE LINKS TABLE NAMESPACE - namespace of Links HBase table,
                      HBASE LINKS COLUMN FAMILY - column family of Links HBase table,
                      PATH TO LINKS HFILE - path to HFile to where new Links will be saved to,
                      
                      HBASE LEGAL UNITS TABLE NAME - name of the HBase table with Legal Units records,
                      HBASE LEGAL UNITS TABLE NAMESPACE - namespace of Legal Unit HBase table,
                      HBASE LEGAL UNITS COLUMN FAMILY - column family of  Legal Unit HBase table,
                      PATH TO LEGAL UNITS HFILE - path to HFile to where new  Units will be saved to,
                      
                      HBASE ENTERPRISE TABLE NAME - name of the HBase table with Enterprise Units records,
                      HBASE ENTERPRISE TABLE NAMESPACE - namespace of Enterprise Unit HBase table,
                      HBASE ENTERPRISE COLUMN FAMILY - column family of Enterprise Unit HBase table,
                      PATH TO ENTERPRISE HFILE - path to HFile to where new Enterprise Units will be saved to,
                      
                      HBASE LOCAL UNITS TABLE NAME - name of the HBase table with Local Units records,
                      HBASE LOCAL UNITS TABLE NAMESPACE - namespace of Local HBase table,
                      HBASE LOCAL UNITS COLUMN FAMILY - column family of  Local Unit HBase table,
                      PATH TO LOCAL UNITS HFILE - path to HFile to where new Local Units will be saved to,
                      
                      HBASE REPORTING UNITS TABLE NAME - name of the HBase table with Reporting Units records,
                      HBASE REPORTING UNITS TABLE NAMESPACE - namespace of Reporting HBase table,
                      HBASE REPORTING UNITS COLUMN FAMILYcolumn family of Reporting Unit HBase table,
                      PATH TO REPORTING UNITS HFILE - path to HFile to where new Reporting Units will be saved to,
                      
                      PATH TO PARQUET FILE - path to parquet input file with BI data,
                      ZOOKEEPER URL - self explainatory,
                      ZOOKEEPER PORT - self explainatory,
                      TIME PERIOD - time period, used to generate HBase table name, which is of format: [UNIT TABLE NAME]_[TIME PERIOD],
                      HIVE DB NAME - Hive db name, currently used for region mappings data storage,
                      REGION-TO-POSTCODE MAPPING HIVE TABLE NAME - Hive table name where region mappings data stored,
                      SHORT REGION-TO-POSTCODE MAPPING HIVE TABLE NAME - Hive table name where short version of region mappings data stored,
                      PATH TO PAYE DATA FILE - path to csv file with PAYE data,
                      PATH TO VAT DATA FILE - path to csv file with VAT data,,
                      ENVIRONMENT - enviromnemt reference, e.g. "local", cluster" etc,
                      ACTION - execution flow. At the time of writing there are 2 types of actions supported: 1."add-calculated-period"
                                                                                                              2. "data-integrity-report"
                      
                      

##HBase table schemas

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
    |    sic07           |    Mandatory       |                                                                                                                            |
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
    +    lurn            |    Mandatory       |  Unique 9 digit ref                                                                                                         |
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
    |    postcode        |    Mandatory       |  average of PAYE jobs across year                                                                                           |
    |    region          |    Mandatory       |  Geographic region allocated through a geography lookup from Postcode (defined as GOR in source file).9 character string    |
    |    sic07           |    Mandatory       |  Sum of Rep VAT turnover for contained rep VAT Groups                                                                       |
    |    employees       |    Mandatory       |  Apportioned Rep VAT turnover based on employees                                                                            |                
    |    employment      |    Mandatory       |                                                                                                                             |
    |--------------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------+