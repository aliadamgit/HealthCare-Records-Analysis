# HealthCare-Records-Analysis
HealthCare Records Analysis with Microsoft Fabric

![Copy of End-to-End Microsoft Fabric Project using Medallion Architecture ](https://github.com/user-attachments/assets/2a2a0a32-7367-400f-86ac-51e1ba9948b0)


## Exective Summary 

![image](https://github.com/user-attachments/assets/cd6c6233-4e12-4cbd-b569-be412ff86bef)


## The pipeline for loading the data from Azure sql database into Bronze lakehouse 


![image](https://github.com/user-attachments/assets/1feef0db-e536-443f-aa09-f1900c5a8724)

```sql
select table_name
from INFORMATION_SCHEMA.TABLES
where table_schema = 'dbo'
and table_type = 'base table'
order by table_name asc;
```

Loading Files into Bronze Lakehouse 

![image](https://github.com/user-attachments/assets/fb1e3278-3802-4eaf-b836-122a90e0772c)

## Load and Transform Data into Silver Lakehouse

```python
# Import the Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
```
## Import encounters Table to Silver

```python
df_encounters = spark.read.table("BronzeLH.encounters")
# display(df_encounters.head(2))
```

```python
from pyspark.sql.functions import to_date, to_timestamp
df_encounters = df_encounters.withColumn("START",col("START").cast("timestamp"))\
                             .withColumn("STOP",col("STOP").cast("timestamp"))
display(df_encounters.head(2))
```
![image](https://github.com/user-attachments/assets/68654195-d724-4800-b0d9-f2389ab600aa)

```python
# Convert to Integer type
df_encounters = df_encounters.withColumn("CODE",col("CODE").cast(IntegerType()))\
                             .withColumn("BASE_ENCOUNTER_COST",col("BASE_ENCOUNTER_COST").cast(FloatType()))\
                             .withColumn("TOTAL_CLAIM_COST",col("TOTAL_CLAIM_COST").cast(FloatType()))\
                             .withColumn("PAYER_COVERAGE",col("PAYER_COVERAGE").cast(FloatType()))
display(df_encounters.head(2))
```

```python
df_encounters = df_encounters.drop("ORGANIZATION","REASONCODE","REASONDESCRIPTION")
display(df_encounters.head(6))
```
![image](https://github.com/user-attachments/assets/f5a274da-99f3-4ca4-b8e8-88b2c187dffa)

```python
# Writing the data to Silver Lakehouse
df_encounters.write.format("delta")\
             .mode("overwrite")\
            .saveAsTable("SilverLH.fact_encounters")
```

## Create encounter class_dim table

```python
df_encounters_class = df_encounters.dropDuplicates(["ENCOUNTERCLASS"]).select("ENCOUNTERCLASS")
display(df_encounters_class.head(8))
```

![image](https://github.com/user-attachments/assets/c9bd6543-6a62-4fd0-b849-524036a78eed)

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
# Create id column 
win_spc = Window.orderBy("ENCOUNTERCLASS")
df_encounters_class = df_encounters_class.withColumn("class_id",row_number().over(win_spc) - 0 )
display(df_encounters_class)
```
![image](https://github.com/user-attachments/assets/3eb1120f-594b-439c-b201-d8b5405e22b8)

```python
# Writing the data to Silver Lakehouse
df_encounters_class.write.format("delta")\
             .mode("overwrite")\
            .saveAsTable("SilverLH.dim_encounters_class")
```

```python
# join the new dim encounter class table with encounter table
df_encounters_join = df_encounters.join(df_encounters_class,df_encounters.ENCOUNTERCLASS == df_encounters_class.ENCOUNTERCLASS,"left")
display(df_encounters_join.head(10))
```

![image](https://github.com/user-attachments/assets/8c066f4a-3cff-41e9-a438-84a21e51b0e6)


```python
# Drop Encounterclass Column
df_encounters_join = df_encounters_join.drop("ENCOUNTERCLASS")
display(df_encounters_join.head(4))
```

```python
# Writing the data to Silver Lakehouse
df_encounters_join.write.format("delta")\
             .mode("overwrite")\
             .option("overwriteSchema", "true")\
            .saveAsTable("SilverLH.fact_encounters")
```

## Load patients Table to Silver Lakehouse

```python
# Read Patients table
df_patients = spark.read.table("BronzeLH.patients")
display(df_patients.head(7))
```

![image](https://github.com/user-attachments/assets/7136a9e1-4958-4ba5-ba11-e663e02cfc89)

```python
df_patients = df_patients.drop("PREFIX","FIRST","LAST","SUFFIX","MAIDEN","ZIP")
display(df_patients.head(4))
```

```python
from pyspark.sql.functions import to_date, to_timestamp
df_patients = df_patients.withColumn("BIRTHDATE",col("BIRTHDATE").cast("timestamp"))\
                             .withColumn("DEATHDATE",col("DEATHDATE").cast("timestamp"))
display(df_patients.head(2))
```
![image](https://github.com/user-attachments/assets/8455c2fd-0e3b-43df-8f61-ce8648522fb3)

## Create Dim City Table

```python
df_patients_city = df_patients.dropDuplicates(["CITY"]).select("CITY")
display(df_patients_city.head(4))
```

![image](https://github.com/user-attachments/assets/7c753966-2146-408b-ab52-4497c896d856)

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
# Create id column for patient City
win_spc = Window.orderBy("CITY")
df_patients_city = df_patients_city.withColumn("city_id",row_number().over(win_spc) - 0 )
df_patients_city = df_patients_city.drop("class_id")
display(df_patients_city)
```

![image](https://github.com/user-attachments/assets/4a4336cd-de51-433d-8ba1-0cdb399a2551)


```python
# Writing the data to Silver Lakehouse
df_patients_city.write.format("delta")\
             .mode("overwrite")\
             .option("overwriteschema","true")\
            .saveAsTable("SilverLH.dim_city")
```

```python
# join the new dim encounter class table with encounter table
df_patients_join = df_patients.join(df_patients_city,df_patients.CITY == df_patients_city.CITY,"left")
display(df_patients_join.head(5))
```

![image](https://github.com/user-attachments/assets/0bc5bf16-abd8-4765-892a-c2e2cb364ae8)


```python
# Drop City Column 
df_patients_join = df_patients_join.drop("CITY")
display(df_patients_join.head(2))
```

```python
# Create Age Column
df_patients_join2 = df_patients_join.withColumn("age", round( datediff( col("DEATHDATE") , col("BIRTHDATE") ) / lit(365.25) , 2))
display(df_patients_join2.head(3))
```

![image](https://github.com/user-attachments/assets/d35f7a23-38ef-4471-bbe9-2cf949aa6ba0)


```python
# Create Date Keys Columns
df_patients_join3 = df_patients_join2.withColumn("BIRTHDATE_KEY", regexp_replace(col("BIRTHDATE").cast("date") , '[-]','').cast(IntegerType()))\
                                     .withColumn("DEATHDATE_KEY", regexp_replace(col("DEATHDATE").cast("date") , '[-]','').cast(IntegerType()))
display(df_patients_join3.head(3))
```

![image](https://github.com/user-attachments/assets/7fcf19ec-0c07-410e-891d-79b81b9dde2a)

```python
# Writing the data to Silver Lakehouse
df_patients_join3.write.format("delta")\
             .mode("overwrite")\
             .option("overwriteschema","true")\
            .saveAsTable("SilverLH.dim_patients")
```

## Transform Payers Table

```python
# Read Payers table
df_payers = spark.read.table("BronzeLH.payers")
display(df_payers.head(4))
```

![image](https://github.com/user-attachments/assets/350b11ea-2597-4647-96f8-a192edce89f3)

```python
# Drop Columns
df_payers = df_payers.drop("NAME","ADDRESS","ZIP","PHONE")
display(df_payers.head(4))
```

```python
# Writing the data to Silver Lakehouse
df_payers.write.format("delta")\
             .mode("overwrite")\
             .option("overwriteschema","true")\
            .saveAsTable("SilverLH.dim_payers")
```

## Load procedures table

```python
# Read procedures 
df_procedures = spark.read.table("BronzeLH.procedures")
display(df_procedures.head(4))
```

![image](https://github.com/user-attachments/assets/f420ce15-29cc-4c7b-9a47-fd2bc88d964c)


```python
# Drop Columns
df_procedures = df_procedures.drop("REASONCODE","REASONDESCRIPTION")
```

```python
# Create Calculated Columns
df_procedures2 = df_procedures.withColumn("START", col("START").cast("timestamp") )\
                              .withColumn("STOP", col("STOP").cast("timestamp") )\
                              .withColumn("PERIOD_HOURS", round( ( col("STOP").cast("long") - col("START").cast("long") ) / (60*60) ,1)  )\
                              .withColumn("START_KEY", regexp_replace(col("START").cast("date") , '[-]','').cast(IntegerType()))
display(df_procedures2.head(3))
```

![image](https://github.com/user-attachments/assets/2cbba239-5849-4196-af75-19a64b845a4f)

```python
# Create Calculated Period class
df_procedures3 = df_procedures2.withColumn("PERIOD_CLASS", when( (df_procedures2.PERIOD_HOURS <= 1),lit("0-1"))  \
                                           .when( (df_procedures2.PERIOD_HOURS <= 2),lit("1.1-2"))\
                                           .when( (df_procedures2.PERIOD_HOURS <= 3),lit("2.1-3"))\
                                          .when( (df_procedures2.PERIOD_HOURS > 3 ),lit("> 3"))
                                          )
display(df_procedures3.head(3))
```

![image](https://github.com/user-attachments/assets/a4354d7d-20e0-4522-9927-3e2a42cc5856)

```python
# Writing the data to Silver Lakehouse
df_procedures3.write.format("delta")\
             .mode("overwrite")\
             .option("overwriteschema","true")\
            .saveAsTable("SilverLH.fact_procedures")
```

## Dim Date Table

```python
# Dim Date Table
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
```

```sql
%%sql
DROP TABLE IF EXISTS SilverLH.dim_date;
CREATE TABLE IF NOT EXISTS SilverLH.dim_date
(
    CalendarDate Date,
    DateKey int,
    YearId int,
    MonthId int,
    DayId int,
    Quarter int,
    ShortDay VARCHAR(3),
    MonthName VARCHAR(12)
) USING DELTA
PARTITIONED BY (YearId)
```

```sql
%%sql
SELECT min(BIRTHDATE) from dim_patients;
```
![image](https://github.com/user-attachments/assets/92e40ba0-5ffa-4411-b382-63ee3c45e2f7)

```python
start_date = '1922-01-01'
end_date   = '2024-12-31'
date_diff = spark.sql("select date_diff('{}','{}')".format(end_date,start_date)).first()[0]
display(date_diff)
```

```python
ID_data = spark.range(0,date_diff)
```

```python
date_data = ID_data.selectExpr("date_add('{}', cast(id as int)) as CalendarDate".format(start_date))
display(date_data.head(2))
```
![image](https://github.com/user-attachments/assets/697073fd-7486-43ce-938c-873121430d51)

```python
date_data.createOrReplaceTempView("DateView")
```

```sql
%%sql
insert into SilverLH.dim_date
SELECT*,
concat( year(CalendarDate) , concat(right (cast( 100 +  month(CalendarDate)  as char(3)),2) ,right (cast( 100 +  day(CalendarDate)  as char(3)),2) ) ) DateKey,
year(CalendarDate) as YearId,
month(CalendarDate) as MonthId,
Day(CalendarDate)   as DayId,
quarter(CalendarDate) as Quarter,
date_format(CalendarDate, 'EEE') as ShortDay,
date_format(CalendarDate, 'MMM') as MonthName
FROM DateView
```

```sql
%%sql
select count(*) from SilverLH.dim_date -- limit 5
```
![image](https://github.com/user-attachments/assets/dfa7e125-e627-40c7-9016-9ac6efd811f2)

## 1. Import fact_encounters Table
We used T-SQL for loading data from silver lakehouse to gold data warehouse 

```sql
-- Dim dim_payers
SELECT TOP 2 [Id],
			[START],
			[STOP],
			[PATIENT],
			[PAYER],
			[CODE],
			[DESCRIPTION],
			[BASE_ENCOUNTER_COST],
			[TOTAL_CLAIM_COST],
			[PAYER_COVERAGE],
			[class_id]
from SilverLH.dbo.fact_encounters;
```

![image](https://github.com/user-attachments/assets/c1a2314e-e9db-4217-b16a-46620be7e792)

## 2. Create Temp Table

```sql
IF object_id('DWH.gold.fact_encounters_new', 'U') IS NOT NULL
BEGIN
    DROP TABLE DWH.gold.fact_encounters_new;
END
CREATE TABLE DWH.gold.fact_encounters_new as 
SELECT      [Id],
			[START],
			[STOP],
			[PATIENT],
			[PAYER],
			[CODE],
			[DESCRIPTION],
			[BASE_ENCOUNTER_COST],
			[TOTAL_CLAIM_COST],
			[PAYER_COVERAGE],
			[class_id]
from SilverLH.dbo.fact_encounters;
```

## 3. Validate New Temp Table

```sql
SELECT TOP 2 * from DWH.gold.fact_encounters_new;
```

![image](https://github.com/user-attachments/assets/8433aebd-2030-4c2e-a8ea-2c56828133a2)

## 4. Swap new table to Production

```sql
IF object_id('DWH.gold.fact_encounters', 'U') IS NOT NULL
BEGIN
    DROP TABLE DWH.gold.fact_encounters;
END
CREATE TABLE DWH.gold.fact_encounters as 
SELECT * from DWH.gold.fact_encounters_new;
```

## 5. Validate Production table

```python
 SELECT TOP 3 * from DWH.gold.fact_encounters;
```

![image](https://github.com/user-attachments/assets/fcf517b4-225e-4ee5-bc3e-8afb420c4e5a)

## 6. Clean Up the temp table

```python
IF object_id('DWH.gold.fact_encounters_new', 'U') IS NOT NULL
BEGIN
    DROP TABLE DWH.gold.fact_encounters_new;
END
```

## The Silver Lakehouse after Transformation 

![image](https://github.com/user-attachments/assets/7a145407-56b1-45d8-8db7-50ea6910991e)

And applay the same logic for all the other tables to form the data warehouse 

![image](https://github.com/user-attachments/assets/52eaa36b-d0de-40df-95dc-094a718c48af)

## Design the data model 

![image](https://github.com/user-attachments/assets/a25bbec8-e735-4b14-b92e-3faa7df574ab)








































































































































