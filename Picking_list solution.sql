--Tạo picking list cho FIFO
DROP TABLE IF EXISTS #Picking_list_FIFO
USE test
SELECT 
b.Item_code,
b.Location,
b.Stock_qty,
b.good_receipt_time,
b.exp_date_time,
CASE
	WHEN [Running Total] < Order_qty THEN Stock_qty
	WHEN [Running Total] >= Order_qty AND LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY good_receipt_time, LEFT(RIGHT(Location, 2),1)) > 0 THEN 
	LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY good_receipt_time, LEFT(RIGHT(Location, 2),1))
	WHEN b.Diff <= 0 AND LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY LEFT(RIGHT(Location, 2),1)) IS NULL THEN b.Order_qty
END [Picking quantity] into #Picking_list_FIFO
FROM(
SELECT 
a.*, 
LEFT(RIGHT(Location, 2),1) [Vị trí],
SUM(Stock_qty) OVER(PARTITION BY a.Item_code ORDER BY good_receipt_time, LEFT(RIGHT(Location, 2),1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) [Running Total],
Orders.Order_qty,
Orders.Order_qty - 
SUM(Stock_qty) OVER(PARTITION BY a.Item_code ORDER BY good_receipt_time, LEFT(RIGHT(Location, 2),1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) Diff
FROM Inventory a
LEFT JOIN Orders ON Orders.Item_code = a.Item_code
WHERE a.Item_code IN (SELECT Item_code FROM Orders WHERE Orders.Rule_xuất_hàng = 'FIFO')) b

SELECT * FROM #Picking_list_FIFO

--Tạo picking list FEFO
USE test
DROP TABLE IF EXISTS #Picking_list_FEFO
SELECT 
b.Item_code,
b.Location,
b.Stock_qty,
b.good_receipt_time,
b.exp_date_time,
CASE
	WHEN [Running Total] < Order_qty THEN Stock_qty
	WHEN [Running Total] >= Order_qty AND LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY exp_date_time DESC, LEFT(RIGHT(Location, 2),1)) > 0 THEN 
	LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY exp_date_time DESC, LEFT(RIGHT(Location, 2),1))
	WHEN b.Diff <= 0 AND LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY LEFT(RIGHT(Location, 2),1)) IS NULL THEN b.Order_qty
END [Picking quantity] into #Picking_list_FEFO
FROM(
SELECT 
a.*, 
LEFT(RIGHT(Location, 2),1) [Vị trí],
SUM(Stock_qty) OVER(PARTITION BY a.Item_code ORDER BY exp_date_time DESC, LEFT(RIGHT(Location, 2),1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) [Running Total],
Orders.Order_qty,
Orders.Order_qty - 
SUM(Stock_qty) OVER(PARTITION BY a.Item_code ORDER BY exp_date_time DESC, LEFT(RIGHT(Location, 2),1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) Diff
FROM Inventory a
LEFT JOIN Orders ON Orders.Item_code = a.Item_code
WHERE a.Item_code IN (SELECT Item_code FROM Orders WHERE Orders.Rule_xuất_hàng = 'FEFO')) b

SELECT * FROM #Picking_list_FEFO

--Tạo picking list LIFO
USE test
DROP TABLE IF EXISTS #Picking_list_LIFO
SELECT 
b.Item_code,
b.Location,
b.Stock_qty,
b.good_receipt_time,
b.exp_date_time,
CASE
	WHEN [Running Total] < Order_qty THEN Stock_qty
	WHEN [Running Total] >= Order_qty AND LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY good_receipt_time DESC, LEFT(RIGHT(Location, 2),1)) > 0 THEN 
	LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY good_receipt_time DESC, LEFT(RIGHT(Location, 2),1))
	WHEN b.Diff <= 0 AND LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY LEFT(RIGHT(Location, 2),1)) IS NULL THEN b.Order_qty
END [Picking quantity] into #Picking_list_LIFO
FROM(
SELECT 
a.*, 
LEFT(RIGHT(Location, 2),1) [Vị trí],
SUM(Stock_qty) OVER(PARTITION BY a.Item_code ORDER BY good_receipt_time DESC, LEFT(RIGHT(Location, 2),1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) [Running Total],
Orders.Order_qty,
Orders.Order_qty - 
SUM(Stock_qty) OVER(PARTITION BY a.Item_code ORDER BY good_receipt_time DESC, LEFT(RIGHT(Location, 2),1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) Diff
FROM Inventory a
LEFT JOIN Orders ON Orders.Item_code = a.Item_code
WHERE a.Item_code IN (SELECT Item_code FROM Orders WHERE Orders.Rule_xuất_hàng = 'LIFO')) b

SELECT * FROM #Picking_list_LIFO

--Tạo picking list theo chỉ định
USE test
DROP TABLE IF EXISTS #Designated_Picking_List
SELECT 
b.Item_code,
b.Location,
b.Stock_qty,
b.good_receipt_time,
b.exp_date_time,
CASE
	WHEN [Running Total] < Order_qty THEN Stock_qty
	WHEN [Running Total] >= Order_qty AND LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY LEFT(RIGHT(Location, 2),1)) > 0 THEN 
	LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY LEFT(RIGHT(Location, 2),1))
	WHEN b.Diff <= 0 AND LAG(b.Diff) OVER(PARTITION BY b.Item_code ORDER BY LEFT(RIGHT(Location, 2),1)) IS NULL THEN b.Order_qty
END [Picking quantity] into #Designated_Picking_List
FROM(
SELECT 
a.*, 
LEFT(RIGHT(Location, 2),1) [Vị trí],
SUM(Stock_qty) OVER(PARTITION BY a.Item_code ORDER BY LEFT(RIGHT(Location, 2),1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) [Running Total],
Orders.Order_qty,
Orders.Order_qty - 
SUM(Stock_qty) OVER(PARTITION BY a.Item_code ORDER BY LEFT(RIGHT(Location, 2),1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) Diff
FROM Inventory a
LEFT JOIN Orders ON Orders.Item_code = a.Item_code
WHERE a.Item_code IN (SELECT Item_code FROM Orders WHERE Orders.Rule_xuất_hàng = '2020-11-01 00:00:00')
AND a.exp_date_time = '2020-11-01 00:00:00') b

SELECT * FROM #Designated_Picking_List

--Số lượng tồn còn lại
SELECT
b.Item_code,
b.Location,
b.latest_exp_date_time,
b.latest_good_receipt_time,
ISNULL(b.Stock_qty_after_picking, Stock_qty) remain_quantity
FROM
(SELECT
a.Item_code,
a.Location,
SUM(a.Stock_qty) Stock_qty,
MAX(good_receipt_time) latest_good_receipt_time,
MAX(a.exp_date_time) latest_exp_date_time,
SUM(a.Stock_qty) - SUM([Picking quantity]) Stock_qty_after_picking
FROM 
(SELECT * FROM #Picking_list_FIFO
UNION ALL
SELECT * FROM #Picking_list_FEFO
UNION ALL
SELECT * FROM #Picking_list_LIFO
UNION ALL
SELECT * FROM #Designated_Picking_List) a
GROUP BY a.Item_code,a.Location) b

SELECT * FROM Inventory
SELECT * FROm Orders