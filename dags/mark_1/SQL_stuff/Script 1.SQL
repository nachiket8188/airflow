/*
This script contains the SQL commands I used during this PoC Project. These range from DDLs to create databases
to simple select statements. In an actual production environment, many of these could be clubbed into Stored Procedures.
*/

create database practice;

use database practice;

create schema retail_sales;

use schema retail_sales;

create table retail_sales.sales_by_category (
    product_category varchar(15),
    total_amount number(8)
);

select
    *
from
    sales_by_category;