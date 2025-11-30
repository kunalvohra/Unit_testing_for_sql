SELECT s.sale_id, p.product_name 
FROM sales s 
JOIN products p 
ON s.product_id = p.product_id;
