/*
Creates table for the exercise and insert values
*/
-- Create Countries table
CREATE TABLE Countries
(
    country_id INT NOT NULL,
    country_name [NVARCHAR](25)  NOT NULL    
    -- specify more columns here
);
GO

-- Create Country table
CREATE TABLE Weather
(
    country_id INT NOT NULL,
    weather_state INT NOT NULL,
    day [DATE]  NOT NULL,     
    -- specify more columns here
);
GO

-- Insert into the Country table
-- Insert rows into table 'Employees'
INSERT INTO Countries
   ([country_id],[country_name])
VALUES
   ( 2, N'USA'),
   ( 3, N'Australia'),
   ( 7, N'Peru'),
   ( 5, N'China'),
   ( 8, N'Morocco'),
   ( 9, N'Spain'),
   ( 2, N'USA')
 
GO

-- Insert into the Weather table

INSERT INTO Weather
   ([country_id],[weather_state], [day])
VALUES
   ( 2, 15, N'2019-11-01'),
   ( 2, 12, N'2019-10-28'),
   ( 2, 12, N'2019-10-27'),
   ( 3, -2, N'2019-11-10'),
   ( 3, 0, N'2019-11-01'),
   ( 3, 3, N'2019-11-12'),
   ( 5, 16, N'2019-11-07'),
   ( 5, 18, N'2019-11-09'),
   ( 5, 21, N'2019-11-23'),
   ( 7, 25, N'2019-11-28'),
   ( 7, 22, N'2019-12-01'),
   ( 7, 20, N'2019-12-02'),
   ( 8, 25, N'2019-11-05'),
   ( 8, 27, N'2019-11-15'),
   ( 8, 31, N'2019-11-25'),
   ( 9, 7, N'2019-11-23'),
   ( 9, 3, N'2019-12-23') 
GO

/*

-- Get a list of tables and views in the current database
SELECT table_catalog [database], table_schema [schema], table_name name, table_type type
FROM INFORMATION_SCHEMA.TABLES
GO 

*/