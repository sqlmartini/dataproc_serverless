CREATE OR REPLACE PROCEDURE adventureworks_raw.Product_merge()
BEGIN
    MERGE adventureworks_raw.Product a
    USING adventureworks_temp.Product b
    ON a.ProductID = b.ProductID
    WHEN NOT MATCHED THEN
        INSERT (ProductID, Name, ProductNumber, MakeFlag, FinishedGoodsFlag, Color, SafetyStockLevel, ReorderPoint, StandardCost, ListPrice, Size, SizeUnitMeasureCode, WeightUnitMeasureCode, Weight, DaysToManufacture, ProductLine, Class, Style, ProductSubcategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate, rowguid, ModifiedDate)
        VALUES (ProductID, Name, ProductNumber, MakeFlag, FinishedGoodsFlag, Color, SafetyStockLevel, ReorderPoint, StandardCost, ListPrice, Size, SizeUnitMeasureCode, WeightUnitMeasureCode, Weight, DaysToManufacture, ProductLine, Class, Style, ProductSubcategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate, rowguid, ModifiedDate)
    WHEN MATCHED THEN
        UPDATE SET 
            Name = b.Name
            , ProductNumber = b.ProductNumber
            , MakeFlag = b.MakeFlag
            , FinishedGoodsFlag = b.FinishedGoodsFlag
            , Color = b.Color
            , SafetyStockLevel = b.SafetyStockLevel
            , ReorderPoint = b.ReorderPoint
            , StandardCost = b.StandardCost
            , ListPrice = b.ListPrice
            , Size = b.Size
            , SizeUnitMeasureCode = b.SizeUnitMeasureCode
            , WeightUnitMeasureCode = b.WeightUnitMeasureCode
            , Weight = b.Weight
            , DaysToManufacture = b.DaysToManufacture
            , ProductLine = b.ProductLine
            , Class = b.Class
            , Style = b.Style
            , ProductSubcategoryID = b.ProductSubcategoryID
            , ProductModelID = b.ProductModelID
            , SellStartDate = b.SellStartDate
            , SellEndDate = b.SellEndDate
            , DiscontinuedDate = b.DiscontinuedDate
            , rowguid = b.rowguid
            , ModifiedDate = b.ModifiedDate;
END;

CREATE OR REPLACE PROCEDURE adventureworks_raw.ProductCategory_merge()
BEGIN
    MERGE adventureworks_raw.ProductCategory a
    USING adventureworks_temp.ProductCategory b
    ON a.ProductCategoryID = b.ProductCategoryID
    WHEN NOT MATCHED THEN
        INSERT (ProductCategoryID, Name, rowguid, ModifiedDate)
        VALUES (ProductCategoryID, Name, rowguid, ModifiedDate)
    WHEN MATCHED THEN
        UPDATE SET 
            Name = b.Name
            , rowguid = b.rowguid
            , ModifiedDate = b.ModifiedDate;
END;

CREATE OR REPLACE PROCEDURE adventureworks_raw.ProductSubcategory_merge()
BEGIN
    MERGE adventureworks_raw.ProductSubcategory a
    USING adventureworks_temp.ProductSubcategory b
    ON a.ProductSubcategoryID = b.ProductSubcategoryID
    WHEN NOT MATCHED THEN
        INSERT (ProductSubcategoryID, ProductCategoryID, Name, rowguid, ModifiedDate)
        VALUES (ProductSubcategoryID, ProductCategoryID, Name, rowguid, ModifiedDate)
    WHEN MATCHED THEN
        UPDATE SET
            ProductCategoryID = b.ProductSubcategoryID
            , Name = b.Name
            , rowguid = b.rowguid
            , ModifiedDate = b.ModifiedDate;
END;

CREATE OR REPLACE PROCEDURE adventureworks_raw.SalesOrderDetail_merge()
BEGIN
    MERGE adventureworks_raw.SalesOrderDetail a
    USING adventureworks_temp.SalesOrderDetail b
    ON a.SalesOrderID = b.SalesOrderID AND a.SalesOrderDetailID = b.SalesOrderDetailID
    WHEN NOT MATCHED THEN
        INSERT (SalesOrderID, SalesOrderDetailID, CarrierTrackingNumber, OrderQty, ProductID, SpecialOfferID, UnitPrice, UnitPriceDiscount, LineTotal, rowguid, ModifiedDate)
        VALUES (SalesOrderID, SalesOrderDetailID, CarrierTrackingNumber, OrderQty, ProductID, SpecialOfferID, UnitPrice, UnitPriceDiscount, LineTotal, rowguid, ModifiedDate)
    WHEN MATCHED THEN
        UPDATE SET
            CarrierTrackingNumber = b.CarrierTrackingNumber
            , OrderQty = b.OrderQty
            , ProductID = b.ProductID
            , SpecialOfferID = b.SpecialOfferID
            , UnitPrice = b.UnitPrice
            , UnitPriceDiscount = b.UnitPriceDiscount
            , LineTotal = b.LineTotal
            , rowguid = b.rowguid
            , ModifiedDate = b.ModifiedDate;
END;

CREATE OR REPLACE PROCEDURE adventureworks_raw.SalesOrderHeader_merge()
BEGIN
    MERGE adventureworks_raw.SalesOrderHeader a
    USING adventureworks_temp.SalesOrderHeader b
    ON a.SalesOrderID = b.SalesOrderID
    WHEN NOT MATCHED THEN
        INSERT (SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber, CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, CreditCardID, CreditCardApprovalCode, CurrencyRateID, SubTotal, TaxAmt, Freight, TotalDue, Comment, rowguid, ModifiedDate)
        VALUES (SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber, CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, CreditCardID, CreditCardApprovalCode, CurrencyRateID, SubTotal, TaxAmt, Freight, TotalDue, Comment, rowguid, ModifiedDate)
        WHEN MATCHED THEN
            UPDATE SET
                RevisionNumber = b.RevisionNumber
                , OrderDate = b.OrderDate
                , DueDate = b.DueDate
                , ShipDate = b.ShipDate
                , Status = b.Status
                , OnlineOrderFlag = b.OnlineOrderFlag
                , SalesOrderNumber = b.SalesOrderNumber
                , PurchaseOrderNumber = b.PurchaseOrderNumber
                , AccountNumber = b.AccountNumber
                , CustomerID = b.CustomerID
                , SalesPersonID = b.SalesPersonID
                , TerritoryID = b.TerritoryID
                , BillToAddressID = b.BillToAddressID
                , ShipToAddressID = b.ShipToAddressID
                , ShipMethodID = b.ShipMethodID
                , CreditCardID = b.CreditCardID
                , CreditCardApprovalCode = b.CreditCardApprovalCode
                , CurrencyRateID = b.CurrencyRateID
                , SubTotal = b.SubTotal
                , TaxAmt = b.TaxAmt
                , Freight = b.Freight
                , TotalDue = b.TotalDue
                , Comment = b.Comment
                , rowguid = b.rowguid
                , ModifiedDate = b.ModifiedDate;
END;

CREATE OR REPLACE PROCEDURE adventureworks_raw.SalesTerritory_merge()
BEGIN
    MERGE adventureworks_raw.SalesTerritory a
    USING adventureworks_temp.SalesTerritory b
    ON a.TerritoryID = b.TerritoryID
    WHEN NOT MATCHED THEN
        INSERT (TerritoryID, Name, CountryRegionCode, `Group`, SalesYTD, SalesLastYear, CostYTD, CostLastYear, rowguid, ModifiedDate)
        VALUES (TerritoryID, Name, CountryRegionCode, `Group`, SalesYTD, SalesLastYear, CostYTD, CostLastYear, rowguid, ModifiedDate)
        WHEN MATCHED THEN
            UPDATE SET
                Name = b.Name
                , CountryRegionCode = b.CountryRegionCode
                , `Group` = b.`Group`
                , SalesYTD = b.SalesYTD
                , SalesLastYear = b.SalesLastYear
                , CostYTD = b.CostYTD
                , CostLastYear = b.CostLastYear
                , rowguid = b.rowguid
                , ModifiedDate = b.ModifiedDate;
END;

CREATE OR REPLACE PROCEDURE adventureworks_raw.MergeAllTables()
BEGIN
    CALL adventureworks_raw.Product_merge();
    CALL adventureworks_raw.ProductCategory_merge();
    CALL adventureworks_raw.ProductSubcategory_merge();
    CALL adventureworks_raw.SalesOrderDetail_merge();
    CALL adventureworks_raw.SalesOrderHeader_merge();
    CALL adventureworks_raw.SalesTerritory_merge();
END;