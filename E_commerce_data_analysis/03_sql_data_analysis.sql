-- 1-1. 전체 주문에서 Cancelled, Shipped, Pending 상태별 주문 개수와 비율은?
-- status별 주문 횟수와 전체 총 주문횟수를 구해 cte에 넣어줬다 
WITH status_cte AS
(
SELECT 
	`status`, 
    COUNT(*) AS status_num_order,
    (SELECT COUNT(*) FROM amazon_sale_report_staging3) AS total_num_order
FROM amazon_sale_report_staging3
GROUP BY `status`
ORDER BY `status`
)

-- cte를 이용해 각 status 별 주문 횟수가 전체 주문 횟수에서 차지하는 비율을 구해줬다 
SELECT 
	`status`,
    status_num_order,
    (status_num_order/total_num_order)*100 AS percentage
FROM status_cte;

-- 1-2. 월별로 배송(Shipped)된 주문의 주문개수, 총 수량(Qty)과 매출(Amount)은?
-- 월별로 묶은 후, 주문개수, 총 수량, 총 매출을 계산해주었다. 
SELECT 
	MONTH(`date`) AS `month`,
    COUNT(*) AS total_num,
	SUM(qty) AS total_qty,
    ROUND(SUM(amount), 2) AS total_amount
FROM amazon_sale_report_staging3
WHERE `status` = 'Shipped' /* 배송이 완료된 주문 */
	AND amount != 0 /* 가격이 표기되지 않은 rows는 분석에 포함하지 않았다 */
    AND MONTH(`date`) != 3   /* 3월은 정보가 불충분해 월별 분석에 포함하지 않았다 */
GROUP BY `month`
ORDER BY `month`;

-- 1-3. 월별로 Cancelled, Shipped, Pending 별 주문 개수와 비율은?
-- 각 월별/status 별 주문 개수를 먼저 계산해준다 
WITH month_percentage AS
(
SELECT 
	MONTH(`date`) AS `month`, 
	`status`, 
    COUNT(*) AS status_num
FROM amazon_sale_report_staging3
WHERE MONTH(`date`) != 3
GROUP BY `month`, `status`
ORDER BY `month`, `status`
)

-- 해당 함수를 불러와 각 월마다 Cancelled, Shipped, Pending의 주문 횟수 비율을 계산한다
SELECT 
	`month`,
    `status`,
    status_num,
    /* subquery를 통해서 각 월마다의 주문 총 횟수를 따로 계산하고, 이를 percentage 계산에 이용했다 */
    ROUND((status_num/(SELECT SUM(status_num) FROM month_percentage m2 WHERE m2.`month` = m1.`month`))*100, 2) AS status_percentage
FROM month_percentage m1
GROUP BY `month`, `status`
ORDER BY `month`, `status`;

-- 2-1. 어떤 카테고리가 가장 많은 주문을 기록했는가?
-- 카테고리 그룹별로 주문횟수를 계산했다
SELECT 
	category,
    COUNT(*) AS total_num
FROM amazon_sale_report_staging3
GROUP BY category
ORDER BY total_num DESC, category;

-- 2-2. 각 카테고리별 취소율(Cancelled 비율)이 가장 높은 상품은 무엇인가?
-- cte를 이용해 각 카테고리의 cancelled 된 주문의 횟수와 전체 주문 횟수를 미리 계산해주었다
WITH percentage_cte AS
(
SELECT
	a1.category,
    COUNT(*) AS cancelled_num,
    /* subquery로 카테고리별 전체 주문 횟수를 따로 계산해주었다 */
    (SELECT COUNT(*) 
		FROM amazon_sale_report_staging3 a2 
        WHERE a2.category = a1.category) AS total_num 
FROM amazon_sale_report_staging3 a1
WHERE `status` = 'Cancelled' 
GROUP BY a1.category
)

-- cte를 이용해 카테고리별로 전체 주문 횟수중에 취소된 주문 횟수가 차지하는 비율을 구했다
SELECT
	category,
    cancelled_num,
    total_num,
    ROUND((cancelled_num/total_num)*100, 2) AS cancelled_percentage
FROM percentage_cte
ORDER BY cancelled_percentage DESC;

-- 3-1. 어떤 주(State)에서 주문이 가장 많았는가?
-- 각 주 별로 주문횟수를 구했다
SELECT 
	ship_state,
    COUNT(*) AS total_num
FROM amazon_sale_report_staging3
GROUP BY ship_state
ORDER BY total_num DESC;

-- 3-2. 각 주(State)에서 가장 많이 팔린 카테고리는 무엇인가?
SELECT
	ship_state,
    category,
    COUNT(*) AS total_num, /* 각 지역과 카테고리 별 주문 횟수를 계산했다 */
    /* 지역 그룹 안에서 주문 횟수가 가장 높은 순서대로 랭킹을 매겼다 */
    RANK() OVER(PARTITION BY ship_state ORDER BY COUNT(*) DESC) AS `rank` 
FROM amazon_sale_report_staging3
GROUP BY ship_state, category
ORDER BY ship_state, `rank`;

-- 위의 표에서 top rank(1위, 2위)만을 따로 뽑아보고자 한다
SELECT
	ship_state,
    category,
    total_num,
    `rank`
FROM
(
SELECT
	ship_state,
    category,
    COUNT(*) AS total_num,
    RANK() OVER(PARTITION BY ship_state ORDER BY COUNT(*) DESC) AS `rank`
FROM amazon_sale_report_staging3
GROUP BY ship_state, category
ORDER BY ship_state, `rank`) AS rank_sub
WHERE `rank` IN (1, 2); /* 각 지역에서 주문 횟수 rank 가 1, 2위인 rows만 뽑았다 */

-- 4-1. 가장 많이 주문된 사이즈는 무엇인가?
SELECT
	size,
    COUNT(*) AS size_num
FROM amazon_sale_report_staging3
GROUP BY size
ORDER BY size_num DESC;

-- 5-1. 가장 많이 판매된 개별 상품 (Top 10 ASIN)은?
SELECT
	asin,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS `rank`, /* 주문 횟수에 따라 순위를 매겼다 */
    COUNT(*) AS asin_num
FROM amazon_sale_report_staging3
GROUP BY asin
ORDER BY asin_num DESC
LIMIT 10; /* top10에 속하는 개별 상품의 정보만 뽑았다 */

-- 6-1. 상품 가격대별 주문량(Shipped Qty)은 어떻게 분포되는가?
SELECT 
	ROUND(amount/qty, 0) AS price_per_unit, /* 개별 상품의 가격대별 주문 횟수를 알아보기 위해 총 가격을 수량으로 나눴다 */
    COUNT(*) AS total_num /* 같은 가격을 가진 상품의 주문 횟수이다 */
FROM amazon_sale_report_staging3
/* amount와 qty가 0이 아닌 rows를 대상으로 분석을 했다 */
WHERE amount != 0
	AND qty != 0
GROUP BY price_per_unit
ORDER BY price_per_unit DESC;

-- 7-1. 전체 주문에서 B2B 주문 비율은?
SELECT
	B2B,
    b2b_num,
    total_num,
    ROUND((b2b_num/total_num)*100, 2) AS percentage /* 아래 섭쿼리로 계산한 정보를 통해 B2B의 비율을 계산했다 */
FROM
(
SELECT B2B,
	COUNT(*) AS b2b_num, /* B2B는 FALSE와 TRUE로 value가 나뉜다. 각각의 주문 횟수를 계산했다 */
    (SELECT COUNT(*) FROM amazon_sale_report_staging3) AS total_num /* subquery로 전체 주문횟수를 미리 계산했다 */
FROM amazon_sale_report_staging3
GROUP BY B2B) b2b_table;

-- 8-1. Fulfillment 방식(Amazon vs Merchant) 비율은?
SELECT
	fulfilment,
    ful_num,
    total_num,
    ROUND((ful_num/total_num)*100, 2) AS percentage /* 아래 섭쿼리로 계산한 정보를 통해 비율을 계산했다 */
FROM
(
SELECT fulfilment,
	COUNT(*) AS ful_num, /* fulfilment는 Amazon과 Merchant로 value가 나뉜다. 각각의 주문 횟수를 계산했다 */
    (SELECT COUNT(*) FROM amazon_sale_report_staging3) AS total_num /* subquery로 전체 주문횟수를 미리 계산했다 */
FROM amazon_sale_report_staging3
GROUP BY fulfilment) ful_table;

-- 9-1. 전체 주문에서 프로모션 코드가 적용된 주문의 횟수와 수익은?
SELECT
	(CASE WHEN promotion_ids = 'null' THEN 'yes'
    ELSE 'no' END) AS promotion, /* promotion_ids는 사용된 promotion_ids의 정보가 담겨있기 때문에 적용/비적용의 여부를
알아보기 위해 해당 columns의 value가 null이면 'no', promotion_ids가 적용되었으면 'yes'가 나오게 했다 */
    COUNT(*) AS total_num,
    ROUND(SUM(amount), 2) AS total_amount
FROM amazon_sale_report_staging3
GROUP BY promotion;

