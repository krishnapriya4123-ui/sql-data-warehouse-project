/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

use mysql;
-- Drop and recreate the 'DataWarehouse' database
DROP database if exists DataWareHouse;

- Create the 'DataWarehouse' database
  
create database DataWareHouse;
use DataWareHouse;

-- Create Schemas

create  database if not exists bronze; 
create database if not exists silver;
create database if not exists gold;
