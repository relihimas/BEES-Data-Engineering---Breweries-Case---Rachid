CREATE TABLE IF NOT EXISTS bronze_breweries (

    id VARCHAR(36) PRIMARY KEY NOT NULL,
    name VARCHAR(255) NOT NULL,
    brewery_type VARCHAR(50) NOT NULL,
    address_1 VARCHAR(255),
    address_2 VARCHAR(255),
    address_3 VARCHAR(255),
    street VARCHAR(255),
    city VARCHAR(100),
    state_province VARCHAR(100),
    state VARCHAR(50), 
    postal_code VARCHAR(20),
    country VARCHAR(100) NOT NULL,
    longitude NUMERIC(10, 7),
    latitude NUMERIC(10, 7),
    phone VARCHAR(50),
    website_url VARCHAR(255),
    source_json_body JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_breweries_name ON bronze_breweries (name);
CREATE INDEX idx_breweries_type ON bronze_breweries (brewery_type);