CREATE TABLE customer (
    id serial PRIMARY KEY,
    first_name VARCHAR (100) NOT NULL,
    last_name VARCHAR (100) NOT NULL,
    email VARCHAR (255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
)

CREATE TABLE account (
    id serial PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
    account_type VARCHAR (50) NOT NULL,
    balance DECIMAL (15, 2) NOT NULL DEFAULT 0.00 CHECK (balance >= 0)  ,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    currency VARCHAR (10) NOT NULL DEFAULT 'USD'
);

CREATE TABLE transaction (
    id serial PRIMARY KEY,
    account_id INT NOT NULL REFERENCES account(id) ON DELETE CASCADE,
    amount DECIMAL (15, 2) NOT NULL CHECK (amount > 0),
    transaction_type VARCHAR (50) NOT NULL,
    transaction_date TIMESTAMP WITH TIME ZONE DEFAULT now(),
    related_account_id INT NULL,
    status VARCHAR (50) NOT NULL DEFAULT 'completed',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);  