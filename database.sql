CREATE TABLE IF NOT EXISTS mines (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    company VARCHAR(100),
    commodity VARCHAR(50),
    region VARCHAR(50),
    lat DECIMAL(10, 6),
    lng DECIMAL(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sensors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mine_id VARCHAR(50),
    sensor_type VARCHAR(50) NOT NULL,
    installation_date DATE,
    last_maintenance DATE,
    status ENUM('active', 'inactive', 'maintenance'),
    FOREIGN KEY (mine_id) REFERENCES mines(id)
);

CREATE TABLE IF NOT EXISTS sensor_readings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sensor_id INT,
    reading_value DECIMAL(10, 4),
    reading_type VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sensor_id) REFERENCES sensors(id)
);

CREATE TABLE IF NOT EXISTS predictions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mine_id VARCHAR(50),
    risk_level ENUM('low', 'medium', 'high'),
    risk_score INT,
    confidence INT,
    predicted_timeframe VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (mine_id) REFERENCES mines(id)
);

CREATE TABLE IF NOT EXISTS alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mine_id VARCHAR(50),
    alert_type VARCHAR(50),
    message TEXT,
    severity ENUM('info', 'warning', 'critical'),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (mine_id) REFERENCES mines(id)
);

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    company VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS seismic_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE,
    magnitude DECIMAL(3, 1),
    place TEXT,
    time DATETIME,
    longitude DECIMAL(9, 6),
    latitude DECIMAL(8, 6),
    depth DECIMAL(8, 2),
    region VARCHAR(255),
    significance INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS weather_data;

CREATE TABLE IF NOT EXISTS weather_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mine_id VARCHAR(50),
    temperature DECIMAL(5, 2),
    rainfall DECIMAL(5, 2),
    humidity DECIMAL(5, 2),
    wind_speed DECIMAL(5, 2),
    `condition` VARCHAR(100),
    recorded_at DATETIME,
    FOREIGN KEY (mine_id) REFERENCES mines(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS geological_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(255),
    rock_type VARCHAR(100),
    hazard_level VARCHAR(50),
    stability VARCHAR(50),
    survey_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);