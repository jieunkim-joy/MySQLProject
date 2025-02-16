-- Data Cleaning

SELECT *
FROM amazon_sale_report;

-- 데이터 전처리용 데이블을 형성
CREATE TABLE amazon_sale_report_staging
LIKE amazon_sale_report;

-- 해당 테이블에 columns이 제대로 들어갔는지 확인 
SELECT *
FROM amazon_sale_report_staging;

-- 해당 테이블에 기존 데이터셋 정보를 전부 삽입한다
INSERT amazon_sale_report_staging
SELECT *
FROM amazon_sale_report;

-- 1. remove duplicates
-- 함수를 통해 이 모든 column에서 같은 value를 가진 rows를 그룹으로 묶고, 이 그룹 안에서 순서를 매긴다. 
-- 2 이상의 순서를 가진 row가 있다면 해당 row는 duplicate을 가진다는 뜻이다.
-- cte를 이용해 row_num이 2 이상인 행을 찾아준다

WITH duplicate_cte AS
(SELECT *,
	ROW_NUMBER() OVER(PARTITION BY order_ID, `date`, `status`, fulfilment, sales_channel, ship_service_level, style, sku, category,
    size, `asin`, courier_status, qty, amount, ship_state, promotion_ids, B2B, fulfilled_by) AS row_num
FROM amazon_sale_report_staging)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- 해당 rows를 delete 하기 전에 row_num을 포함한 새로운 테이블을 만든다
CREATE TABLE amazon_sale_report_staging2 (
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
	fulfilled_by VARCHAR(255),
    row_num INT);

INSERT INTO amazon_sale_report_staging2
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY order_ID, `date`, `status`, fulfilment, sales_channel, ship_service_level, style, sku, category,
    size, `asin`, courier_status, qty, amount, ship_state, promotion_ids, B2B, fulfilled_by) AS row_num
FROM amazon_sale_report_staging;

-- 삭제할 rows들을 한번더 체크한다
SELECT *
FROM amazon_sale_report_staging2
WHERE row_num > 1;

-- 해당 rows를 DELETE
DELETE
FROM amazon_sale_report_staging2
WHERE row_num > 1;

-- DUPLICATE를 삭제한 후 필요없어진 row_num column은 DROP
ALTER TABLE amazon_sale_report_staging2
DROP COLUMN row_num;

-- 2. standardizing data
-- 2-1. ship_state column의 문제 발견 
SELECT DISTINCT ship_state
FROM amazon_sale_report_staging2
ORDER BY ship_state;

-- TRIM을 통해 불필요한 빈 공간을 제거해준다 
UPDATE amazon_sale_report_staging2
SET ship_state = TRIM(ship_state);

-- ship_state의 문자열을 모두 대문자로 바꿔주었다 
UPDATE amazon_sale_report_staging2
SET ship_state = UPPER(ship_state);

/* 같은 state인데 오타 등의 이유로 다른 문자열을 가진 데이터 value들을 통일해주었다. */
/* ARUNACHAL PRADESH % AR : ARUNACHAL PRADESH로 통일 */
UPDATE amazon_sale_report_staging2
SET ship_state = 'ANDHRA PRADESH'
WHERE ship_state = 'AR';

/* DELHI & NEW DELHI : DELHI로 통일 */
UPDATE amazon_sale_report_staging2
SET ship_state = 'DELHI'
WHERE ship_state LIKE '%DELHI%';

/* NAGALAND & NL : NAGALAND로 통일 */
UPDATE amazon_sale_report_staging2
SET ship_state = 'NAGALAND'
WHERE ship_state = 'NL';

/* ODISHA & ORISSA : ODISHA로 통일 */
UPDATE amazon_sale_report_staging2
SET ship_state = 'ODISHA'
WHERE ship_state = 'ORISSA';

/* PUNJAB & PB & PUNJAB/MOHALI/ZIRAKPUR : PUNZAB로 통일 */
UPDATE amazon_sale_report_staging2
SET ship_state = 'PUNJAB'
WHERE ship_state = 'PB' OR ship_state = 'PUNJAB/MOHALI/ZIRAKPUR';

/* RAJASTHAN & RAJSHTHAN & RAJSTHAN & RJ : RAJASTHAN으로 통일 */
UPDATE amazon_sale_report_staging2
SET ship_state = 'RAJASTHAN'
WHERE ship_state LIKE 'RAJ%' or ship_state = 'RJ';

-- 2-2. `status` column
-- 기존의 status column은 총 13개의 값으로 구성되어있다. 
-- 하지만 분석의 용의성을 위해 이 column의 데이터를 총 세가지의 카테고리[Cancelled, shipped, pending]​​로 나눠 교체하였다

-- 데이터를 바꾸기 전에, 새로운 테이블을 추가해주었다
DROP TABLE IF EXISTS amazon_sale_report_staging3;
CREATE TABLE amazon_sale_report_staging3 (
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
FROM amazon_sale_report_staging3;

INSERT INTO amazon_sale_report_staging3
SELECT *
FROM amazon_sale_report_staging2;

-- 각 카테고리에 맞춰 `status`의 정보를 새롭게 UPDATE 해준다
UPDATE amazon_sale_report_staging3
SET `status` = 'Cancelled'
WHERE `status` IN ('Cancelled', 'Shipped - Damaged', 'Shipped - Lost in Transit', 'Shipped - Rejected by Buyer', 'Shipped - Returned to Seller', 'Shipped - Returning to Seller');

UPDATE amazon_sale_report_staging3
SET `status` = 'Shipped'
WHERE `status` IN ('Shipped', 'Shipped - Delivered to Buyer', 'Shipped - Out for Delivery', 'Shipped - Picked Up', 'Shipping');

UPDATE amazon_sale_report_staging3
SET `status` = 'Pending'
WHERE `status` IN ('Pending', 'Pending - Waiting for Pick Up');

-- 카테고리가 잘 적용되었는지 체크
SELECT DISTINCT
	`status`
FROM amazon_sale_report_staging3;

-- 3. NULL 혹은 BLANK value 처리하기
-- 참고 : 데이터 import 과정에서 csv. 파일의 빈칸들이 많은 에러를 일으켜서 이 빈칸을 0이나 'null'로 대체시켰다
-- 아예 필요없는 정보는 (신중하게) 제거하고, 정확하게 채워넣을 수 있는 정보는 채워넣기

-- 3-1. status가 shipped이지만 qty는 0인 rows를 체크
-- 주문 정보는 shipped이지만 수량이 0으로 나오는 row들이다. 입력 과정에서 오류가 있었던 것으로 추측할 수 있다.
-- 해당 조건을 충족하는 rows는 100개이기 때문에 그대로 남겨둬도 결과에 큰 영향을 주지 않겠지만, 그래도 제거하기를 택했다.

-- 제거할 rows들 확인하기 
SELECT 
	COUNT(*)
FROM amazon_sale_report_staging2
WHERE `status` = 'Shipped'
    AND qty = 0;

-- 해당 rows들을 제거하기
DELETE 
FROM amazon_sale_report_staging3
WHERE `status` = 'Shipped'

-- 3-2. status는 shipped이지만 amount = 0인 rows들
-- 위와 비슷하게 주문정보는 shipped이지만 amount(가격)은 0으로 나오는 rows들이다. 
-- 해당 조건에 충족하는 rows들은 1600개 정도로, 그냥 제거하기엔 수가 많아서 최대한 정보를 채울 수 있는 것들을 채우기로 했다.

-- JOIN을 통해 asin(아마존 등록번호)와 promotion_ids가 같고 amount 정보가 채워져있는 행들을 amount 정보가 없는 행에 연결했다.

-- amount의 정보가 없는 행은 하나이지만, 그 행과 asin과 promotion_ids가 같은 행은 여러개일 가능성이 있다. 
-- 그렇기에 `index`를 GROUP BY 하고 AVG()를 사용해 amoun 정보가 없는 행 하나에 
-- 그와 비슷한 정보를 가진 행들의 amount 평균값이 제공되도록 코드를 작성했다. 
-- (1600개 중에서 이 방식으로 정보를 채울 수 있는 rows는 그리 많지 않았다.)

SELECT 
	a1.`index`,
    a1.amount,
    ROUND(AVG(a2.amount), 2) AS avg_amount
FROM amazon_sale_report_staging3 a1
	INNER JOIN amazon_sale_report_staging3 a2 
		ON a2.asin = a1.asin
            AND a2.promotion_ids = a1.promotion_ids
            ANd a2.qty = a1.qty
WHERE a1.`status` = 'Shipped'
    AND a1.amount = 0
    AND a2.amount != 0
GROUP BY a1.`index`;

-- 해당 평균값을 amount 정보가 없는 행에 넣어주었다
UPDATE amazon_sale_report_staging3 a1
	INNER JOIN (
		SELECT a1.`index`,
			a1.amount,
			ROUND(AVG(a2.amount), 2) AS avg_amount
		FROM amazon_sale_report_staging3 a1
			LEFT JOIN amazon_sale_report_staging3 a2 
				ON a2.asin = a1.asin
					AND a2.promotion_ids = a1.promotion_ids
                    AND a2.qty = a1.qty
		WHERE a1.`status` = 'Shipped'
			AND a1.amount = 0
			AND a2.amount != 0 
		GROUP BY a1.`index`) AS avg_values
		ON a1.`index` = avg_values.`index`
SET a1.amount = avg_values.avg_amount;

