CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE accounts (
	id SERIAL PRIMARY KEY,
	account_type VARCHAR(20) NOT NULL, -- savings, checking, credit
	balance DECIMAL(18, 2) NOT NULL DEFAULT 0 CHECK (balance >= 0), -- 18 digit numbers include 2 after decimal point
	currency CHAR(3) NOT NULL DEFAULT 'VND',
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE customers_accounts (
	customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
	account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
	role VARCHAR(20) NOT NULL CHECK (ROLE IN ('PRIMARY_OWNER', 'CO_OWNER')),
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	PRIMARY KEY (customer_id, account_id) -- composite PK
);

CREATE TABLE transactions (
	id SERIAL PRIMARY KEY,
	account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
	transaction_type VARCHAR(20) NOT NULL,
	amount DECIMAL(18, 2) NOT NULL CHECK(amount > 0),
	related_account INT NOT NULL,
	description VARCHAR(255),
	status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);