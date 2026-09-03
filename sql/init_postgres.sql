CREATE TABLE customers (
  id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, city TEXT, tier TEXT NOT NULL DEFAULT 'bronze',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE products (
  id SERIAL PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL, price NUMERIC(10,2) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE orders (
  id BIGSERIAL PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers(id), status TEXT NOT NULL,
  total_amount NUMERIC(12,2) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE order_items (
  id BIGSERIAL PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders(id), product_id INT NOT NULL REFERENCES products(id),
  quantity INT NOT NULL, unit_price NUMERIC(10,2) NOT NULL);

-- full row images on UPDATE/DELETE so Debezium emits complete "before" payloads
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

INSERT INTO customers(name, email, city, tier)
SELECT 'Customer '||g, 'user'||g||'@example.com',
       (ARRAY['Chennai','Bengaluru','Hyderabad','Visakhapatnam','Pune','Mumbai'])[1 + g % 6], 'bronze'
FROM generate_series(1, 500) g;

INSERT INTO products(name, category, price)
SELECT 'Product '||g, (ARRAY['electronics','grocery','fashion','home','sports','books'])[1 + g % 6],
       round((random() * 4950 + 49)::numeric, 2)
FROM generate_series(1, 300) g;
