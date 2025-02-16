
-- Data import
USE sales_data;

-- 1. 새로운 테이블 만들기
DROP TABLE IF EXISTS amazon_sale_report;
CREATE TABLE amazon_sale_report (
	`index` INT,	
	order_ID VARCHAR(255),
	`date` DATE,
	`status` VARCHAR(255),
    fulfilment VARCHAR(255),
    sales_channel VARCHAR(255),
	ship_service_level VARCHAR(255),
	style VARCHAR(255),
	sku VARCHAR(255),
	category VARCHAR(255),
	size VARCHAR(255),
	`asin` VARCHAR(255),
	courier_status VARCHAR(255),
	qty	INT,	
	amount DOUBLE,
	ship_state VARCHAR(255),
	promotion_ids MEDIUMBLOB,
	B2B	VARCHAR(255),
	fulfilled_by VARCHAR(255));
    
SELECT *
FROM amazon_sale_report;

-- 2. Amazon Sale Report csv. 파일 해당 테이블에 LOAD하기
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Amazon Sale Report.csv" INTO TABLE amazon_sale_report
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- 테이블 내용 확인
SELECT *
FROM amazon_sale_report
WHERE courier_status = 'null';

-- 테이블 개수 확인
SELECT COUNT(*)
FROM amazon_sale_report;
