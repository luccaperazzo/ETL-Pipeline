CREATE TABLE IF NOT EXISTS sales_orders (
    order_id BIGINT PRIMARY KEY,
    customer TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    date DATE NOT NULL
);
