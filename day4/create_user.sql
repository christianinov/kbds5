-- 3. создать своего юзера + схему
CREATE LOGIN khristianinov WITH PASSWORD = '##################'
GO

CREATE SCHEMA khristianinov_schema
GO

CREATE USER khristianinov
	FOR LOGIN khristianinov
	WITH DEFAULT_SCHEMA = khristianinov_schema
GO

EXEC sp_addrolemember N'db_owner', N'khristianinov'
GO
