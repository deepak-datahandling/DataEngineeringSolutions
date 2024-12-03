# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime,timedelta

# Initialize Spark Session
spark = SparkSession/builder.appName("DatabrikcsQuery").getOrCreate()

#Set up the Databricks Connection properties for PROD
databricks_host="<PROD Databricks URL>"
http_path = "<SQL Path taken from Cluster JDBC Connection details>"

#PROD Token

dbutils.widgets.text("ProdToken","")
ProdToken=dbutils.widgets.get("ProdToken")

# %%
tblist=spark.sql("""
select * from <db>.<tble> where in_scope='Y' and BatchID=1
                 """)

display(tblist)

# %%
# Keeping Select Query with and Without Where Clause
def sel_query_change(az_tble,tble):
    global selQuery,selQueryWhere
    selQuery="SELECT * FROM " + str(az_tble)
    selQueryWhere= (
        "SELECT * FROM " + str(az_tble)+ " WHERE " + str(tble["WhereConditionStmt"])
    )

#PROD Access Issue Handling
def prod_access_issue(az_view,tble,flag):
    global prod_df
    sle_query_change(az_view,tble)
    if flag ==1:
        query=selQueryWhere
    else:
        query=selQuery
    print(query)
    prod_df=(
        spark.read.format("databricks")
        .option("host",databricks_host)
        .option("httpPath",http_path)
        .option("personalAccesssToken",f{ProdToken})
        .option("query",f"{query}")
        .load()
    )
    prod_df.createOrReplaceTempView("az_prod")



# %%
cnt=0
t_1=datetime.now()

#Iterate Over the list of tables
for tble in tblist.collect():
    cmnt=""
    where_flag=[1 if tble["WhereConditionStmt"] !=None else 0]
    az_tble=tble["AzureTableName"]
    azure_table_name=tble["AzureSchemaName"]+"."+tble["AzureTableName"]
    col_list=tble["ColumnNameList"]
    Validation_check=spark.sql(
        f""" select * from <db>.<val_results> where AzureTableName='{az_tble}'
        and logdate=current_Date()
        """
    )
    cnt +=1
    # Validation Begin for the current date:
    if int(Validation_check.count()) !=1:
        t_2=datetime.now()
        if tble["WhereConditionStmt"] != None:
            try:
                sel_query_change(azure_table_name,tble)
                dev_df=spark.sql(selQueryWhere)
                dev_df.createOrReplaceTempView("az_dev")
                print('Dev Data Extracted')
            except Exception as e:
                cmnt = "Table Not Present"
                print(f"{azure_table_name} Table Not Present in Dev")

            try:
                sel_query_change(azure_table_name,tble)
                prod_df=(
                    spark.read.format("databricks")
                    .option("host",databricks_host)
                    .option("httpPath",http_path)
                    .option("personalAccesssToken",f{ProdToken})
                    .option("query",f"{selQueryWhere}")
                    .load()
                    )
                prod_df.createOrReplaceTempView("az_prod")
                print('Prod Data Extracted')
            except Exception as e:
                cmnt = "Table Not Present"
                print(f"{azure_table_name} Table Not Present in Prod")
        
        elif tble["WhereConditionStmt"] == None:
            try:
                sel_query_change(azure_table_name,tble)
                dev_df=spark.sql(selQuery)
                dev_df.createOrReplaceTempView("az_dev")
                print('Dev Data Extracted')
            except Exception as e:
                cmnt = "Table Not Present"
                print(f"{azure_table_name} Table Not Present in Dev")

            try:
                sel_query_change(azure_table_name,tble)
                prod_df=(
                    spark.read.format("databricks")
                    .option("host",databricks_host)
                    .option("httpPath",http_path)
                    .option("personalAccesssToken",f{ProdToken})
                    .option("query",f"{selQuery}")
                    .load()
                    )
                prod_df.createOrReplaceTempView("az_prod")
                print('Prod Data Extracted')
            except Exception as e:
                cmnt = "Table Not Present"
                print(f"{azure_table_name} Table Not Present in Prod")
        
        if cmnt != "Table Not Present":
            print('Validation in Progress')
            dev_prod_diff = spark.sql(
                f""" select * from az_dev
                minus
                select * from az_prod """
            )
            prod_dev_diff=spark.sql(
                f""" select * from az_prod
                minus
                select * from az_dev """
            )
            ProdCount=int(prod_df.count())
            DevCount=int(dev_def.count())
            AzProdDev_Recs_diff=int(prod_dev_diff.count())
            AzDevProd_Recs_diff=int(dev_prod_diff.count())
        
        else:
            ProdCount=-1
            DevCount=-1
            AzProdDev_Recs_diff=-1
            AzDevProd_Recs_diff=-1
        
        Status = [
            "PASSED"
            if AzProdDev_Recs_diff ==0 and AzDevProd_Recs_diff==0
            else "FAILED"
        ]
        log_date = datetime.now().strftime("%Y-%m-%d")

        spark.sql(
            f""" Insert into <db>.<val_results>
            select
            AzureSchemaName,
            AzureTableName,
            SourceSystem,
            TargetSystem,
            '{ProdCount}',
            '{DevCount}',
            '{AzProdDev_Recs_diff}',
            '{AzDevProd_Recs_diff}',
            '{cmnt}',
            cast({log_date} as date),
            '{Status[0]}' from <db>.<tblist> where AzureTableName='{az_tble}'
            """
        )

        elapsed_time=(datetime.min + (datetime.now() - t_1)).time()
        t_1=datetime.now()

        spark.sql(
            f"""
            Insert into <db>.<runtime_audit>
            Select
            AzureSchemaName,
            AzureTableName,
            '{t_2}',
            '{t_1}',
            '{elapsed_time}' from <db>.<tblist> where AzureTableName='{az_tble}'
            """
        )

        print(
            f"{azure_table_name} ({cnt} of {tblist.count()}) Table Validation Completed and took {elapsed_time}"
        )
    else:
        print(f" Validation Already Processed for {az_tble} Table")






