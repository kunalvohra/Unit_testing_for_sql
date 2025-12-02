SELECT s.sale_id, p.product_name 
FROM db.sales s 
JOIN db.products p 
ON s.product_id = p.product_id;
