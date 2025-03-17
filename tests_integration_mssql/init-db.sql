-- Create a new database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'HRDEMO')
BEGIN
    CREATE DATABASE HRDEMO;
END
GO

-- Switch to the new database
USE HRDEMO;
GO

-- Ensure the table exists before inserting data
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Employee Group Mapping')
BEGIN
    CREATE TABLE [Employee Group Mapping] (
        Version NVARCHAR(32) NOT NULL,
        Employee NVARCHAR(32) NOT NULL,
        [Group] NVARCHAR(32) NOT NULL
    );
END
GO

-- Declare variables (no GO here)
DECLARE @Version1 NVARCHAR(32) = 'DataCopy SQL Test 1';
DECLARE @Version2 NVARCHAR(32) = 'DataCopy SQL Test 2';
DECLARE @Counter INT = 1;

-- Insert sample data for Version1
WHILE @Counter <= 50
BEGIN
    INSERT INTO [Employee Group Mapping] ([Version], [Employee], [Group])
    VALUES (
        @Version1,
        'Employee_' + RIGHT('0' + CAST(@Counter AS NVARCHAR(2)), 2),
        'Group_' + CAST(1 + ABS(CHECKSUM(NEWID())) % 5 AS NVARCHAR(1))
    );

    SET @Counter = @Counter + 1;
END

-- Reset counter for next batch
SET @Counter = 1;

-- Insert sample data for Version2
WHILE @Counter <= 50
BEGIN
    INSERT INTO [Employee Group Mapping] ([Version], [Employee], [Group])
    VALUES (
        @Version2,
        'Employee_' + RIGHT('0' + CAST(@Counter AS NVARCHAR(2)), 2),
        'Group_' + CAST(1 + ABS(CHECKSUM(NEWID())) % 5 AS NVARCHAR(1))
    );

    SET @Counter = @Counter + 1;
END
GO
