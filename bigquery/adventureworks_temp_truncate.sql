CREATE OR REPLACE PROCEDURE adventureworks_temp.TruncateTables()
BEGIN
    TRUNCATE TABLE adventureworks_temp.Product;
    TRUNCATE TABLE adventureworks_temp.ProductCategory;
    TRUNCATE TABLE adventureworks_temp.ProductSubcategory;
    TRUNCATE TABLE adventureworks_temp.SalesOrderDetail;
    TRUNCATE TABLE adventureworks_temp.SalesOrderHeader;
    TRUNCATE TABLE adventureworks_temp.SalesTerritory;
END;