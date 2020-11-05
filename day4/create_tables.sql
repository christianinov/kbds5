-- 5 создать екстернал data source
CREATE DATABASE SCOPED CREDENTIAL khristianinov_AzureStorageCredential
WITH
  IDENTITY = 'lesson02str02',
  SECRET = '###################'
GO

CREATE EXTERNAL DATA SOURCE khristianinov_blob
WITH
  ( LOCATION = 'abfss://nyt@lesson02str02.dfs.core.windows.net' ,
    CREDENTIAL = khristianinov_AzureStorageCredential ,
    TYPE = HADOOP
  )
GO

-- 6 Создать екстернал таблицу для файла  yellow_tripdata_2020-01 на основе екстернал data source
CREATE EXTERNAL FILE FORMAT [file_format_khristianinov]
WITH (FORMAT_TYPE = DELIMITEDTEXT,
      FORMAT_OPTIONS (FIELD_TERMINATOR = N',',
                      FIRST_ROW = 2))
GO

CREATE EXTERNAL TABLE [khristianinov_schema].[khristianinov_ext_table]
(
	[VendorID] [int],
	[tpep_pickup_datetime] [datetime],
	[tpep_dropoff_datetime] [datetime],
	[passenger_count] [int],
	[Trip_distance] [real],
	[RatecodeID] [int],
	[store_and_fwd_flag] [char](1),
	[PULocationID] [int],
	[DOLocationID] [int],
	[payment_type] [int],
	[fare_amount] [real],
	[extra] [real],
	[mta_tax] [real],
	[tip_amount] [real],
	[tolls_amount] [real],
	[improvement_surcharge] [real],
	[total_amount] [real],
	[congestion_surcharge] [real]
)
WITH (DATA_SOURCE = [khristianinov_blob],
      LOCATION = N'/yellow_tripdata_2020-01.csv',
      FILE_FORMAT = [file_format_khristianinov])
GO

-- 7 Выгрузить данные из external table в таблицу "ВашаСхема".fact_tripdata
CREATE TABLE [khristianinov_schema].[fact_tripdata]
WITH (CLUSTERED COLUMNSTORE INDEX,
      DISTRIBUTION =  HASH( [payment_type] ))
AS
SELECT *
FROM [khristianinov_schema].[khristianinov_ext_table]
GO

-- 8 Создать таблицы справочники на основе документа data_dictionary_trip_records_yellow(вручную)
-- Включая дополнительное задание
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[khristianinov_schema].[Vendor]') AND type in (N'U'))
DROP TABLE [khristianinov_schema].[Vendor]
GO

CREATE TABLE [khristianinov_schema].[Vendor] WITH (DISTRIBUTION = REPLICATE,
                                                   CLUSTERED COLUMNSTORE INDEX) AS
SELECT ISNULL(ID, VendorID) AS ID,
       Name
FROM
  (SELECT DISTINCT VendorID AS ID
   FROM [khristianinov_schema].[fact_tripdata]
   WHERE VendorID IS NOT NULL ) AS ids
FULL JOIN
  (SELECT 1 AS VendorID,
          'Creative Mobile Technologies, LLC' AS Name
   UNION ALL
   SELECT 2, 'VeriFone Inc.') AS vendor ON ids.ID = vendor.VendorID
GO


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[khristianinov_schema].[RateCode]') AND type in (N'U'))
DROP TABLE [khristianinov_schema].[RateCode]
GO

CREATE TABLE [khristianinov_schema].[RateCode] WITH (DISTRIBUTION = REPLICATE,
                                                     CLUSTERED COLUMNSTORE INDEX) AS
SELECT ISNULL(ID, RatecodeID) AS ID,
       Name
FROM
  (SELECT DISTINCT RatecodeID AS ID
   FROM [khristianinov_schema].[fact_tripdata]
   WHERE RatecodeID IS NOT NULL) AS ids
FULL JOIN
  (SELECT 1 AS RatecodeID,
          'Standard' AS Name
   UNION ALL SELECT 2, 'JFK'
   UNION ALL SELECT 3, 'Newark'
   UNION ALL SELECT 4, 'Nassau or Westchester'
   UNION ALL SELECT 5, 'Negotiated fare'
   UNION ALL SELECT 6, 'Group ride') AS Ratecode ON ids.ID = Ratecode.RatecodeID
GO


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[khristianinov_schema].[Payment_type]') AND type in (N'U'))
DROP TABLE [khristianinov_schema].[Payment_type]
GO

CREATE TABLE [khristianinov_schema].[Payment_type] WITH (DISTRIBUTION = REPLICATE,
                                                         CLUSTERED COLUMNSTORE INDEX) AS
SELECT ISNULL(ID, payment_type) AS ID,
       Name
FROM
  (SELECT DISTINCT payment_type AS ID
   FROM [khristianinov_schema].[fact_tripdata]
   WHERE payment_type IS NOT NULL) AS ids
FULL JOIN
  (SELECT 1 AS payment_type,
          'Credit card' AS Name
   UNION ALL SELECT 2, 'Cash'
   UNION ALL SELECT 3, 'No charge'
   UNION ALL SELECT 4, 'Dispute'
   UNION ALL SELECT 5, 'Unknown'
   UNION ALL SELECT 6, 'Voided trip') AS Payment_type ON ids.ID = Payment_type.payment_type
GO
