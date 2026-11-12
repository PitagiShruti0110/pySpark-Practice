# Databricks notebook source
# MAGIC %md
# MAGIC ###Data Reading JSON

# COMMAND ----------

df_json = spark.read.format('json')\
    .option('inferSchema',True)\
    .option('header',True)\
    .option('multiLine',False)\
    .load('/Volumes/sparttable/default/saprk_shru/drivers.json')    
    

# COMMAND ----------

df_json.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('/Volumes/sparttable/default//saprk_shru')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/Volumes/sparttable/default/saprk_shru/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
         Item_Identifier string,
         Item_Weight STRING,
         Item_Fat_Content string,
         Item_Visibility double,
         Item_Type string,
         Item_MRP double,
         Outlet_Identifier string,
         Outlet_Establishment_Year integer,
         Outlet_Size string,
         Outlet_Location_Type string,
         Outlet_Type string,
         Item_Outlet_Sales double
'''

# COMMAND ----------

df = spark.read.format('csv')\
    .schema(my_ddl_schema)\
    .option('header',True)\
    .load('/Volumes/sparttable/default/saprk_shru/BigMart Sales.csv')


# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

my_strct_schema = StructType([
                                StructField('Item_Identifier',StringType(),True),
                                StructField('Item_Weight',StringType(),True),
                                StructField('Item_Fat_Content',StringType(),True),
                                StructField('Item_Visibility',StringType(),True),
                                StructField('Item_Type',StringType(),True),
                                StructField('Item_MRP',StringType(),True),
                                StructField('Outlet_Identifier',StringType(),True),
                                StructField('Outlet_Establishment_Year',StringType(),True),
                                StructField('Outlet_Size',StringType(),True),
                                StructField('Outlet_Location_Type',StringType(),True),
                                StructField('Outlet_Type',StringType(),True),
                                StructField('Item_Outlet_Sales',StringType(),True   )

])

# COMMAND ----------

df = spark.read.format('csv')\
    .schema(my_strct_schema)\
    .option('header',True)\
    .load('/Volumes/sparttable/default/saprk_shru/BigMart Sales.csv')
df.printSchema()
df.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

df_sel = df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content'))
df_sel.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###FILTER

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 2

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight').cast('double')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin(['Tier 1','Tier 2']))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 1

# COMMAND ----------

df = df.withColumn('flag',lit("new")).display()

# COMMAND ----------

# DBTITLE 1,withColumn - Scenario 1
df.withColumn('multiply',col('Item_Weight').cast('double')*col('Item_MRP').cast('double')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","L F")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting

# COMMAND ----------

df = df.withColumn('Item_Weight',col("Item_Weight").cast(StringType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 1 

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario -2

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Sceanrio - 3

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 4

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP_DUPLICATES

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2

# COMMAND ----------

df.dropDuplicates(subset = ['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION and UNION BYNAME

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing Dataframes

# COMMAND ----------

data1 = [('1,','kad'),
         ('2','sid')]
schema1 = 'id STRING,name STRING'

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
         ('4','jas')]
schema2 = 'id STRING, name STRING'

df2 = spark.createDataFrame(data2,schema2)
          

# COMMAND ----------

# MAGIC %md
# MAGIC ####UNION

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('kad','1'),
         ('sid','2')]
schema1 = 'name STRING,id STRING'

df1 = spark.createDataFrame(data1,schema1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####UNION BY NAME

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###STRING FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initcap()

# COMMAND ----------

df.select(upper('Item_Type').alias('upper_Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATE FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Current Date

# COMMAND ----------

df = df.withColumn('curr_date',current_date())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date_Add()

# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date',7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Date_Sub()

# COMMAND ----------

df.withColumn('week_before',date_sub('curr_date',7)).display()

# COMMAND ----------

df = df.withColumn('week Before',date_add('curr_date',-7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Diff

# COMMAND ----------

df = df.withColumn('datediff',datediff('week_after','curr_date'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATE_FORMAT

# COMMAND ----------

df = df.withColumn('week Before',date_format('week Before','dd-MM-yyyy'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###HANDLING NULLS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Droping Nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filling Nulls

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SPLIT FUNCTION

# COMMAND ----------

# MAGIC %md
# MAGIC ####SPLIT

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Indexing

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###EXPLODE

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARRAY_CONTAINS

# COMMAND ----------

df_exp.display()

# COMMAND ----------

df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Group By

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 1

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 2

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP').alias('AVG_ITEM_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 3 

# COMMAND ----------

df.groupBy("Item_Type","Outlet_Size").agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 4

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum("Item_MRP").alias("Total_MRP"),avg('Item_MRP').alias('AVG_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###COLLECT_LIST

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string , book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book').alias('books')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot("Outlet_Size").agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN PTHERWISE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2

# COMMAND ----------

df = df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non_veg').otherwise('Veg'))

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 2

# COMMAND ----------

df.withColumn('veg_exp_flag',when((col('veg_flag')=='Veg') & (col('Item_MRP').cast('double')<100),'Veg_InExpensive')\
    .when((col('veg_flag')=='Veg') & (col('Item_MRP').cast('double')>100),'Veg_Expensive')\
    .otherwise('Non_Veg')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS

# COMMAND ----------

dataj1 = [('1', 'gaur', 'd01'),
          ('2', 'kit', 'd02'),
          ('3', 'paul', 'd03'),
          ('4', 'john', 'd03'),
          ('5', 'paul', 'd05'),
          ('6', 'paul', 'd06')  ]

schemaj1 ='empid string, name string, deptid string'

df1 = spark.createDataFrame(dataj1,schemaj1)

df1.display()
dataj2 = [('d01', 'HR'),
          ('d02', 'Marketing'),
          ('d03', 'Accounts'),
          ('d04', 'IT'),
          ('d05', 'Finance')]

schemaj2 = 'deptid string, deptname string'

df2 = spark.createDataFrame(dataj2,schemaj2)

df2.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Inner Join

# COMMAND ----------

df1.join(df2,df2.deptid==df1.deptid,'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Left Join

# COMMAND ----------

df1.join(df2,df1.deptid==df2.deptid,'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right JOIN

# COMMAND ----------

df1.join(df2,df1.deptid==df2.deptid,'right').display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI JOIN

# COMMAND ----------

df1.join(df2,df1.deptid==df2.deptid,'anti').display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Full JOIN

# COMMAND ----------

df1.join(df2,df1.deptid==df2.deptid,'full').display()

# COMMAND ----------

df1.join(df2,df1.deptid==df2.deptid,'cross').display()
df1.crossJoin(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTION

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROW NUMBER

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### RANK()

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Outlet_Sales')).partitionBy(col('Item_Identifier'))))\
             .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_identifier')))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cumulative SUM
# MAGIC

# COMMAND ----------

df.withColumn('cum sum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cum sum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### USE DEFINED FUNCTIONS(UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 1

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC ####STep - 2

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

df.withColumn('mynewcol',my_udf(col('Item_MRP'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING

# COMMAND ----------

# MAGIC %md
# MAGIC #### CSV(comma sepearted value)

# COMMAND ----------

df.write.format('csv')\
    .save('/Volumes/sparttable/default/saprk_shru/mycsv/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### APPEND

# COMMAND ----------

df.write.format('csv')\
    .mode('append')\
    .save('/Volumes/sparttable/default/saprk_shru/mycsv/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ####OVERWRITE

# COMMAND ----------

df.write.format('csv')\
    .mode('overwrite')\
    .option('path','/Volumes/sparttable/default/saprk_shru/mycsv/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####ERROR

# COMMAND ----------

df.write.format('csv')\
    .mode('error')\
    .option('path','/Volumes/sparttable/default/saprk_shru/mycsv/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####IGONRE

# COMMAND ----------

df.write.format('csv')\
    .mode('ignore')\
    .option('path','/Volumes/sparttable/default/saprk_shru/mycsv/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### PARQUET

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path','/Volumes/sparttable/default/saprk_shru/mycsv/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Table
# MAGIC

# COMMAND ----------

df.write\
    .mode('overwrite')\
    .saveAsTable('my_table')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### CreateTempView 

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content = 'Low Fat'

# COMMAND ----------

df_sql = spark.sql('select * from my_view')
df_sql.display()

# COMMAND ----------

# MAGIC %md
# MAGIC