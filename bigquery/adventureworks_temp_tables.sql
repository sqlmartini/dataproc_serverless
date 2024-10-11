CREATE TABLE adventureworks_temp.Product
(
    ProductID INTEGER
    , Name STRING
    , ProductNumber STRING
    , MakeFlag BOOLEAN
    , FinishedGoodsFlag BOOLEAN
    , Color STRING
    , SafetyStockLevel INTEGER
    , ReorderPoint INTEGER
    , StandardCost NUMERIC(19, 4)
    , ListPrice NUMERIC(19, 4)
    , Size STRING
    , SizeUnitMeasureCode STRING
    , WeightUnitMeasureCode STRING
    , Weight NUMERIC(8, 2)
    , DaysToManufacture INTEGER
    , ProductLine STRING
    , Class STRING
    , Style STRING
    , ProductSubcategoryID INTEGER
    , ProductModelID INTEGER
    , SellStartDate TIMESTAMP
    , SellEndDate TIMESTAMP
    , DiscontinuedDate TIMESTAMP
    , rowguid STRING
    , ModifiedDate TIMESTAMP
);

CREATE TABLE adventureworks_temp.ProductCategory
(
    ProductCategoryID INTEGER
    , Name STRING
    , rowguid STRING
    , ModifiedDate TIMESTAMP
);

CREATE TABLE adventureworks_temp.ProductSubcategory
(
    ProductSubcategoryID INTEGER
    , ProductCategoryID INTEGER    
    , Name STRING
    , rowguid STRING
    , ModifiedDate TIMESTAMP
);

CREATE TABLE adventureworks_temp.SalesOrderDetail
(
    SalesOrderID INTEGER
    , SalesOrderDetailID INTEGER
    , CarrierTrackingNumber STRING
    , OrderQty INTEGER
    , ProductID INTEGER
    , SpecialOfferID INTEGER
    , UnitPrice NUMERIC(19, 4)
    , UnitPriceDiscount NUMERIC(19, 4)
    , LineTotal BIGNUMERIC(38, 6)
    , rowguid STRING
    , ModifiedDate TIMESTAMP
);

CREATE TABLE adventureworks_temp.SalesOrderHeader
(
    SalesOrderID INTEGER
    , RevisionNumber INTEGER
    , OrderDate TIMESTAMP
    , DueDate TIMESTAMP
    , ShipDate TIMESTAMP
    , Status INTEGER
    , OnlineOrderFlag BOOLEAN
    , SalesOrderNumber STRING
    , PurchaseOrderNumber STRING
    , AccountNumber STRING
    , CustomerID INTEGER
    , SalesPersonID INTEGER
    , TerritoryID INTEGER
    , BillToAddressID INTEGER
    , ShipToAddressID INTEGER
    , ShipMethodID INTEGER
    , CreditCardID INTEGER
    , CreditCardApprovalCode STRING
    , CurrencyRateID INTEGER
    , SubTotal NUMERIC(19, 4)
    , TaxAmt NUMERIC(19, 4)
    , Freight NUMERIC(19, 4)
    , TotalDue NUMERIC(19, 4)
    , Comment STRING
    , rowguid STRING
    , ModifiedDate TIMESTAMP
);

CREATE TABLE adventureworks_temp.SalesTerritory
(
    TerritoryID INTEGER
    , Name STRING
    , CountryRegionCode STRING
    , `Group`STRING
    , SalesYTD NUMERIC(19, 4)
    , SalesLastYear NUMERIC(19, 4)
    , CostYTD NUMERIC(19, 4)
    , CostLastYear NUMERIC(19, 4)
    , rowguid STRING
    , ModifiedDate TIMESTAMP
);