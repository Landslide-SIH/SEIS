require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const mysql = require('mysql2/promise');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const app = express();
const port = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key';

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

// Database configuration
const dbConfig = {
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'devbansal',
  password: process.env.DB_PASSWORD || 'Jb1525@jb',
  database: process.env.DB_NAME || 'seis_ai',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

let pool;

// Initialize database connection
async function initializePool() {
  try {
    pool = mysql.createPool(dbConfig);
    console.log('Connected to MySQL database');
    await initDatabase();
  } catch (error) {
    console.error('Error connecting to MySQL:', error);
    setTimeout(initializePool, 2000); // Retry after 2 seconds
  }
}

// Initialize database tables
async function initDatabase() {
  try {
    const connection = await pool.getConnection();
    
    // Create tables based on database.sql
    const tables = [
      `CREATE TABLE IF NOT EXISTS mines (
        id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        company VARCHAR(100),
        commodity VARCHAR(50),
        region VARCHAR(50),
        lat DECIMAL(10, 6),
        lng DECIMAL(10, 6),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )`,
      
      `CREATE TABLE IF NOT EXISTS sensors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        mine_id VARCHAR(50),
        sensor_type VARCHAR(50) NOT NULL,
        installation_date DATE,
        last_maintenance DATE,
        status ENUM('active', 'inactive', 'maintenance'),
        FOREIGN KEY (mine_id) REFERENCES mines(id)
      )`,
      
      `CREATE TABLE IF NOT EXISTS sensor_readings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sensor_id INT,
        reading_value DECIMAL(10, 4),
        reading_type VARCHAR(50),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sensor_id) REFERENCES sensors(id)
      )`,
      
      `CREATE TABLE IF NOT EXISTS predictions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        mine_id VARCHAR(50),
        risk_level ENUM('low', 'medium', 'high'),
        risk_score INT,
        confidence INT,
        predicted_timeframe VARCHAR(50),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (mine_id) REFERENCES mines(id)
      )`,
      
      `CREATE TABLE IF NOT EXISTS alerts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        mine_id VARCHAR(50),
        alert_type VARCHAR(50),
        message TEXT,
        severity ENUM('info', 'warning', 'critical'),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        acknowledged BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (mine_id) REFERENCES mines(id)
      )`,
      
      `CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL,
        name VARCHAR(255),
        company VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      
      `CREATE TABLE IF NOT EXISTS seismic_data (
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
      )`,
      
      `CREATE TABLE IF NOT EXISTS weather_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        mine_id VARCHAR(50),
        temperature DECIMAL(5, 2),
        rainfall DECIMAL(5, 2),
        humidity DECIMAL(5, 2),
        wind_speed DECIMAL(5, 2),
        \`condition\` VARCHAR(100),
        recorded_at DATETIME,
        FOREIGN KEY (mine_id) REFERENCES mines(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      
      `CREATE TABLE IF NOT EXISTS geological_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        region VARCHAR(255),
        rock_type VARCHAR(100),
        hazard_level VARCHAR(50),
        stability VARCHAR(50),
        survey_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`
    ];
    
    for (const tableSql of tables) {
      try {
        await connection.execute(tableSql);
      } catch (error) {
        console.error('Error creating table:', error.message);
      }
    }
    
    connection.release();
    console.log('Database tables initialized');
  } catch (error) {
    console.error('Error initializing database:', error);
  }
}

// Initialize database connection
initializePool();

// Authentication middleware
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    req.user = user;
    next();
  });
};

// API Routes

// Health check endpoint
app.get('/api/health', async (req, res) => {
  try {
    await pool.execute('SELECT 1');
    res.json({ status: 'OK', database: 'Connected' });
  } catch (error) {
    res.status(500).json({ status: 'Error', database: 'Disconnected' });
  }
});

// Serve the main page
app.get('/', (req, res) => {
  res.sendFile(__dirname + '/index.html');
});

// Get all mines
app.get('/api/mines', authenticateToken, async (req, res) => {
  try {
    const [rows] = await pool.execute(`
      SELECT m.*, p.risk_level, p.risk_score, p.confidence, p.timestamp as prediction_timestamp
      FROM mines m
      LEFT JOIN predictions p ON m.id = p.mine_id
      WHERE p.timestamp = (SELECT MAX(timestamp) FROM predictions WHERE mine_id = m.id)
      OR p.timestamp IS NULL
    `);
    
    res.json(rows);
  } catch (error) {
    console.error('Error fetching mines:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get specific mine details
app.get('/api/mines/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    const [mineRows] = await pool.execute('SELECT * FROM mines WHERE id = ?', [id]);
    
    if (mineRows.length === 0) {
      return res.status(404).json({ error: 'Mine not found' });
    }
    
    const [predictionRows] = await pool.execute(
      'SELECT * FROM predictions WHERE mine_id = ? ORDER BY timestamp DESC LIMIT 7',
      [id]
    );
    
    const [sensorRows] = await pool.execute(
      `SELECT sr.*, s.sensor_type 
       FROM sensor_readings sr 
       JOIN sensors s ON sr.sensor_id = s.id 
       WHERE s.mine_id = ? 
       ORDER BY sr.timestamp DESC LIMIT 50`,
      [id]
    );
    
    const [alertRows] = await pool.execute(
      'SELECT * FROM alerts WHERE mine_id = ? ORDER BY timestamp DESC LIMIT 10',
      [id]
    );
    
    const [weatherRows] = await pool.execute(
      'SELECT * FROM weather_data WHERE mine_id = ? ORDER BY recorded_at DESC LIMIT 24',
      [id]
    );
    
    res.json({
      mine: mineRows[0],
      predictions: predictionRows,
      sensor_readings: sensorRows,
      alerts: alertRows,
      weather_data: weatherRows
    });
  } catch (error) {
    console.error('Error fetching mine details:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// User registration
app.post('/api/register', async (req, res) => {
  try {
    const { email, password, name, company } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }
    
    // Check if user already exists
    const [existingUsers] = await pool.execute('SELECT id FROM users WHERE email = ?', [email]);
    
    if (existingUsers.length > 0) {
      return res.status(409).json({ error: 'User already exists' });
    }
    
    // Hash password
    const hashedPassword = await bcrypt.hash(password, 10);
    
    // Create user
    const [result] = await pool.execute(
      'INSERT INTO users (email, password, name, company) VALUES (?, ?, ?, ?)',
      [email, hashedPassword, name, company]
    );
    
    // Generate JWT token
    const token = jwt.sign({ userId: result.insertId, email }, JWT_SECRET, { expiresIn: '24h' });
    
    res.status(201).json({
      message: 'User created successfully',
      token,
      user: { id: result.insertId, email, name, company }
    });
  } catch (error) {
    console.error('Error registering user:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// User login
app.post('/api/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }
    
    // Find user
    const [users] = await pool.execute('SELECT * FROM users WHERE email = ?', [email]);
    
    if (users.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    const user = users[0];
    
    // Check password
    const validPassword = await bcrypt.compare(password, user.password);
    
    if (!validPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    // Generate JWT token
    const token = jwt.sign({ userId: user.id, email }, JWT_SECRET, { expiresIn: '24h' });
    
    res.json({
      message: 'Login successful',
      token,
      user: { id: user.id, email: user.email, name: user.name, company: user.company }
    });
  } catch (error) {
    console.error('Error logging in:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Weather data endpoint
app.get('/api/weather/:mineId', authenticateToken, async (req, res) => {
  try {
    const { mineId } = req.params;
    
    // Get mine coordinates
    const [mines] = await pool.execute('SELECT lat, lng FROM mines WHERE id = ?', [mineId]);
    
    if (mines.length === 0) {
      return res.status(404).json({ error: 'Mine not found' });
    }
    
    const mine = mines[0];
    
    // Use real weather API if key is available
    if (process.env.WEATHER_API_KEY) {
      try {
        const weatherResponse = await axios.get(
          `https://api.openweathermap.org/data/2.5/weather?lat=${mine.lat}&lon=${mine.lng}&appid=${process.env.WEATHER_API_KEY}&units=metric`
        );
        
        const weatherData = {
          temperature: weatherResponse.data.main.temp,
          rainfall: weatherResponse.data.rain ? weatherResponse.data.rain['1h'] || 0 : 0,
          humidity: weatherResponse.data.main.humidity,
          windSpeed: weatherResponse.data.wind.speed,
          condition: weatherResponse.data.weather[0].main
        };
        
        // Store weather data
        await pool.execute(
          `INSERT INTO weather_data (mine_id, temperature, rainfall, humidity, wind_speed, condition, recorded_at)
           VALUES (?, ?, ?, ?, ?, ?, NOW())`,
          [mineId, weatherData.temperature, weatherData.rainfall, weatherData.humidity, 
           weatherData.windSpeed, weatherData.condition]
        );
        
        return res.json(weatherData);
      } catch (apiError) {
        console.error('Weather API error:', apiError.message);
        // Fall through to mock data
      }
    }
    
    // Fallback to mock data
    const weatherData = {
      temperature: 25 + (Math.random() * 15),
      rainfall: Math.random() * 30,
      humidity: 30 + (Math.random() * 50),
      windSpeed: Math.random() * 20,
      condition: ['Sunny', 'Cloudy', 'Rainy'][Math.floor(Math.random() * 3)]
    };
    
    // Store mock weather data
    await pool.execute(
      `INSERT INTO weather_data (mine_id, temperature, rainfall, humidity, wind_speed, condition, recorded_at)
       VALUES (?, ?, ?, ?, ?, ?, NOW())`,
      [mineId, weatherData.temperature, weatherData.rainfall, weatherData.humidity, 
       weatherData.windSpeed, weatherData.condition]
    );
    
    res.json(weatherData);
  } catch (error) {
    console.error('Error fetching weather data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Seismic data endpoint
app.get('/api/seismic/:region', authenticateToken, async (req, res) => {
  try {
    const { region } = req.params;
    
    // Use USGS API to get real seismic data
    try {
      const seismicResponse = await axios.get(
        `https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&limit=10&minmagnitude=2.5`
      );
      
      const seismicData = {
        events: seismicResponse.data.features.map(feature => ({
          magnitude: feature.properties.mag,
          depth: feature.geometry.coordinates[2],
          timestamp: new Date(feature.properties.time).toISOString(),
          location: feature.properties.place,
          longitude: feature.geometry.coordinates[0],
          latitude: feature.geometry.coordinates[1]
        }))
      };
      
      // Store seismic events in database
      for (const event of seismicData.events) {
        try {
          await pool.execute(
            `INSERT INTO seismic_data (event_id, magnitude, place, time, longitude, latitude, depth, region, significance)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) 
             ON DUPLICATE KEY UPDATE magnitude=VALUES(magnitude), time=VALUES(time)`,
            [event.timestamp + event.location, event.magnitude, event.location, event.timestamp,
             event.longitude, event.latitude, event.depth, region, Math.floor(event.magnitude * 10)]
          );
        } catch (dbError) {
          console.error('Error storing seismic event:', dbError);
        }
      }
      
      return res.json(seismicData);
    } catch (apiError) {
      console.error('Seismic API error:', apiError.message);
      // Fall through to mock data
    }
    
    // Fallback to mock data
    const seismicData = {
      events: [
        {
          magnitude: 2.5 + (Math.random() * 3),
          depth: 5 + (Math.random() * 15),
          timestamp: new Date(Date.now() - Math.random() * 86400000).toISOString(),
          location: `Near ${['City A', 'City B', 'City C'][Math.floor(Math.random() * 3)]}`,
          longitude: -70 + (Math.random() * 10),
          latitude: -30 + (Math.random() * 10)
        },
        {
          magnitude: 1.8 + (Math.random() * 2),
          depth: 3 + (Math.random() * 10),
          timestamp: new Date(Date.now() - Math.random() * 172800000).toISOString(),
          location: `Near ${['City D', 'City E', 'City F'][Math.floor(Math.random() * 3)]}`,
          longitude: -70 + (Math.random() * 10),
          latitude: -30 + (Math.random() * 10)
        }
      ]
    };
    
    // Store mock seismic events
    for (const event of seismicData.events) {
      try {
        await pool.execute(
          `INSERT INTO seismic_data (event_id, magnitude, place, time, longitude, latitude, depth, region, significance)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [event.timestamp + event.location, event.magnitude, event.location, event.timestamp,
           event.longitude, event.latitude, event.depth, region, Math.floor(event.magnitude * 10)]
        );
      } catch (dbError) {
        console.error('Error storing seismic event:', dbError);
      }
    }
    
    res.json(seismicData);
  } catch (error) {
    console.error('Error fetching seismic data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Prediction endpoint
app.post('/api/predict/:mineId', authenticateToken, async (req, res) => {
  try {
    const { mineId } = req.params;
    const { sensorData } = req.body;
    
    // Get mine data
    const [mines] = await pool.execute('SELECT * FROM mines WHERE id = ?', [mineId]);
    
    if (mines.length === 0) {
      return res.status(404).json({ error: 'Mine not found' });
    }
    
    const mine = mines[0];
    
    // Get additional data for prediction
    const [seismicRows] = await pool.execute(
      `SELECT magnitude, depth, significance 
       FROM seismic_data 
       WHERE region LIKE ? 
       ORDER BY time DESC LIMIT 10`,
      [`%${mine.region}%`]
    );
    
    const [weatherRows] = await pool.execute(
      `SELECT temperature, rainfall, humidity, wind_speed, condition 
       FROM weather_data 
       WHERE mine_id = ? 
       ORDER BY recorded_at DESC LIMIT 24`,
      [mineId]
    );
    
    const [geoRows] = await pool.execute(
      `SELECT hazard_level, stability 
       FROM geological_data 
       WHERE region LIKE ? 
       ORDER BY survey_date DESC LIMIT 1`,
      [`%${mine.region}%`]
    );
    
    // Simple risk prediction logic (replace with actual ML model)
    const riskScore = Math.floor(Math.random() * 100);
    let riskLevel = 'low';
    if (riskScore > 70) riskLevel = 'high';
    else if (riskScore > 40) riskLevel = 'medium';
    
    const prediction = {
      level: riskLevel,
      riskScore: riskScore,
      confidence: Math.floor(Math.random() * 100),
      predictedTimeframe: `${Math.floor(Math.random() * 72) + 1} hours`
    };
    
    // Store prediction in database
    const [result] = await pool.execute(
      `INSERT INTO predictions (mine_id, risk_level, risk_score, confidence, predicted_timeframe) 
       VALUES (?, ?, ?, ?, ?)`,
      [mineId, prediction.level, prediction.riskScore, prediction.confidence, prediction.predictedTimeframe]
    );
    
    // Create alert if risk is high
    if (prediction.level === 'high') {
      await pool.execute(
        `INSERT INTO alerts (mine_id, alert_type, message, severity) 
         VALUES (?, ?, ?, ?)`,
        [mineId, 'risk_alert', `High risk predicted: ${prediction.predictedTimeframe}`, 'critical']
      );
    }
    
    res.json({
      success: true,
      prediction,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error generating prediction:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add new mine
app.post('/api/mines', authenticateToken, async (req, res) => {
  try {
    const { id, name, company, commodity, region, lat, lng } = req.body;
    
    if (!id || !name || !region) {
      return res.status(400).json({ error: 'ID, name, and region are required' });
    }
    
    const [result] = await pool.execute(
      'INSERT INTO mines (id, name, company, commodity, region, lat, lng) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [id, name, company, commodity, region, lat, lng]
    );
    
    res.status(201).json({
      message: 'Mine added successfully',
      mine: { id, name, company, commodity, region, lat, lng }
    });
  } catch (error) {
    console.error('Error adding mine:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add sensor data
app.post('/api/sensor-data', authenticateToken, async (req, res) => {
  try {
    const { mine_id, sensor_type, reading_value, reading_type } = req.body;
    
    if (!mine_id || !sensor_type || reading_value === undefined) {
      return res.status(400).json({ error: 'Missing required fields' });
    }
    
    // First, check if sensor exists
    const [sensors] = await pool.execute(
      'SELECT id FROM sensors WHERE mine_id = ? AND sensor_type = ?',
      [mine_id, sensor_type]
    );
    
    let sensorDbId;
    if (sensors.length === 0) {
      // Create new sensor
      const [sensorResult] = await pool.execute(
        'INSERT INTO sensors (mine_id, sensor_type) VALUES (?, ?)',
        [mine_id, sensor_type]
      );
      sensorDbId = sensorResult.insertId;
    } else {
      sensorDbId = sensors[0].id;
    }
    
    // Add sensor reading
    const [result] = await pool.execute(
      'INSERT INTO sensor_readings (sensor_id, reading_value, reading_type) VALUES (?, ?, ?)',
      [sensorDbId, reading_value, reading_type]
    );
    
    res.status(201).json({
      message: 'Sensor data added successfully',
      reading_id: result.insertId
    });
  } catch (error) {
    console.error('Error adding sensor data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get sensor data for a mine
app.get('/api/sensor-data/:mineId', authenticateToken, async (req, res) => {
  try {
    const { mineId } = req.params;
    const { sensor_type, limit = 50 } = req.query;
    
    let query = `
      SELECT sr.*, s.sensor_type 
      FROM sensor_readings sr 
      JOIN sensors s ON sr.sensor_id = s.id 
      WHERE s.mine_id = ?
    `;
    
    const params = [mineId];
    
    if (sensor_type) {
      query += ' AND s.sensor_type = ?';
      params.push(sensor_type);
    }
    
    query += ' ORDER BY sr.timestamp DESC LIMIT ?';
    params.push(parseInt(limit));
    
    const [rows] = await pool.execute(query, params);
    
    res.json(rows);
  } catch (error) {
    console.error('Error fetching sensor data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add geological data
app.post('/api/geological-data', authenticateToken, async (req, res) => {
  try {
    const { region, rock_type, hazard_level, stability, survey_date } = req.body;
    
    if (!region || !rock_type) {
      return res.status(400).json({ error: 'Region and rock type are required' });
    }
    
    const [result] = await pool.execute(
      `INSERT INTO geological_data (region, rock_type, hazard_level, stability, survey_date) 
       VALUES (?, ?, ?, ?, ?) 
       ON DUPLICATE KEY UPDATE hazard_level=VALUES(hazard_level), stability=VALUES(stability), survey_date=VALUES(survey_date)`,
      [region, rock_type, hazard_level, stability, survey_date]
    );
    
    res.status(201).json({
      message: 'Geological data added successfully',
      id: result.insertId
    });
  } catch (error) {
    console.error('Error adding geological data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get geological data
app.get('/api/geological-data', authenticateToken, async (req, res) => {
  try {
    const [rows] = await pool.execute('SELECT * FROM geological_data ORDER BY survey_date DESC');
    res.json(rows);
  } catch (error) {
    console.error('Error fetching geological data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Train ML model endpoint
app.post('/api/train-model', authenticateToken, async (req, res) => {
  try {
    // Simple placeholder for model training
    res.json({
      message: 'Model training completed',
      accuracy: 0.85,
      modelInfo: 'Placeholder model'
    });
  } catch (error) {
    console.error('Error training model:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get alerts
app.get('/api/alerts', authenticateToken, async (req, res) => {
  try {
    const { acknowledged } = req.query;
    
    let query = `
      SELECT a.*, m.name as mine_name 
      FROM alerts a 
      JOIN mines m ON a.mine_id = m.id
    `;
    
    const params = [];
    
    if (acknowledged !== undefined) {
      query += ' WHERE a.acknowledged = ?';
      params.push(acknowledged === 'true');
    }
    
    query += ' ORDER BY a.timestamp DESC LIMIT 50';
    
    const [rows] = await pool.execute(query, params);
    
    res.json(rows);
  } catch (error) {
    console.error('Error fetching alerts:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Acknowledge alert
app.put('/api/alerts/:id/acknowledge', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    const [result] = await pool.execute(
      'UPDATE alerts SET acknowledged = TRUE WHERE id = ?',
      [id]
    );
    
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Alert not found' });
    }
    
    res.json({ message: 'Alert acknowledged successfully' });
  } catch (error) {
    console.error('Error acknowledging alert:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add this endpoint to server.js (with other API endpoints)
app.post('/api/rockfall-data', authenticateToken, async (req, res) => {
  try {
    const { mine_id, rainfall, temperature, slope_angle, seismic_magnitude, crack_width, risk_score, risk_level, recorded_at } = req.body;
    
    if (!mine_id || risk_score === undefined) {
      return res.status(400).json({ error: 'Mine ID and risk score are required' });
    }
    
    const [result] = await pool.execute(
      `INSERT INTO rockfall_data (mine_id, rainfall, temperature, slope_angle, seismic_magnitude, crack_width, risk_score, risk_level, recorded_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [mine_id, rainfall, temperature, slope_angle, seismic_magnitude, crack_width, risk_score, risk_level, recorded_at || new Date()]
    );
    
    res.status(201).json({
      message: 'Rockfall data added successfully',
      id: result.insertId
    });
  } catch (error) {
    console.error('Error adding rockfall data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add this endpoint to get rockfall data
app.get('/api/rockfall-data/:mineId', authenticateToken, async (req, res) => {
  try {
    const { mineId } = req.params;
    const { limit = 100 } = req.query;
    
    const [rows] = await pool.execute(
      'SELECT * FROM rockfall_data WHERE mine_id = ? ORDER BY recorded_at DESC LIMIT ?',
      [mineId, parseInt(limit)]
    );
    
    res.json(rows);
  } catch (error) {
    console.error('Error fetching rockfall data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Update the prediction endpoint to use rockfall data
app.post('/api/predict/:mineId', authenticateToken, async (req, res) => {
  try {
    const { mineId } = req.params;
    const { sensorData } = req.body;
    
    // Get mine data
    const [mines] = await pool.execute('SELECT * FROM mines WHERE id = ?', [mineId]);
    
    if (mines.length === 0) {
      return res.status(404).json({ error: 'Mine not found' });
    }
    
    const mine = mines[0];
    
    // Get additional data for prediction
    const [seismicRows] = await pool.execute(
      `SELECT magnitude, depth, significance 
       FROM seismic_data 
       WHERE region LIKE ? 
       ORDER BY time DESC LIMIT 10`,
      [`%${mine.region}%`]
    );
    
    const [weatherRows] = await pool.execute(
      `SELECT temperature, rainfall, humidity, wind_speed, condition 
       FROM weather_data 
       WHERE mine_id = ? 
       ORDER BY recorded_at DESC LIMIT 24`,
      [mineId]
    );
    
    const [geoRows] = await pool.execute(
      `SELECT hazard_level, stability 
       FROM geological_data 
       WHERE region LIKE ? 
       ORDER BY survey_date DESC LIMIT 1`,
      [`%${mine.region}%`]
    );
    
    // Get rockfall data for prediction
    const [rockfallRows] = await pool.execute(
      `SELECT rainfall, temperature, slope_angle, seismic_magnitude, crack_width, risk_score, risk_level
       FROM rockfall_data 
       WHERE mine_id = ? 
       ORDER BY recorded_at DESC LIMIT 50`,
      [mineId]
    );
    
    // Enhanced risk prediction logic using rockfall data
    let riskScore = Math.floor(Math.random() * 100);
    
    // If we have rockfall data, use it to influence the prediction
    if (rockfallRows.length > 0) {
      const recentRisk = rockfallRows[0].risk_score;
      const avgRisk = rockfallRows.reduce((sum, row) => sum + row.risk_score, 0) / rockfallRows.length;
      
      // Weighted average of current prediction and historical data
      riskScore = Math.floor((riskScore * 0.4) + (recentRisk * 0.3) + (avgRisk * 0.3));
    }
    
    let riskLevel = 'low';
    if (riskScore > 70) riskLevel = 'high';
    else if (riskScore > 40) riskLevel = 'medium';
    
    const prediction = {
      level: riskLevel,
      riskScore: riskScore,
      confidence: Math.floor(Math.random() * 100),
      predictedTimeframe: `${Math.floor(Math.random() * 72) + 1} hours`
    };
    
    // Store prediction in database
    const [result] = await pool.execute(
      `INSERT INTO predictions (mine_id, risk_level, risk_score, confidence, predicted_timeframe) 
       VALUES (?, ?, ?, ?, ?)`,
      [mineId, prediction.level, prediction.riskScore, prediction.confidence, prediction.predictedTimeframe]
    );
    
    // Create alert if risk is high
    if (prediction.level === 'high') {
      await pool.execute(
        `INSERT INTO alerts (mine_id, alert_type, message, severity) 
         VALUES (?, ?, ?, ?)`,
        [mineId, 'risk_alert', `High risk predicted: ${prediction.predictedTimeframe}`, 'critical']
      );
    }
    
    res.json({
      success: true,
      prediction,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error generating prediction:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add this endpoint to server.js
app.post('/api/rockfall-data/bulk', authenticateToken, async (req, res) => {
    try {
        const rockfallDataArray = req.body;
        
        if (!Array.isArray(rockfallDataArray)) {
            return res.status(400).json({ error: 'Expected an array of rockfall data' });
        }
        
        if (rockfallDataArray.length === 0) {
            return res.status(400).json({ error: 'Empty data array' });
        }
        
        const results = [];
        for (const data of rockfallDataArray) {
            const { mine_id, rainfall, temperature, slope_angle, seismic_magnitude, crack_width, risk_score, risk_level, recorded_at } = data;
            
            if (!mine_id || risk_score === undefined) {
                results.push({ error: 'Mine ID and risk score are required' });
                continue;
            }
            
            try {
                const [result] = await pool.execute(
                    `INSERT INTO rockfall_data (mine_id, rainfall, temperature, slope_angle, seismic_magnitude, crack_width, risk_score, risk_level, recorded_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [mine_id, rainfall, temperature, slope_angle, seismic_magnitude, crack_width, risk_score, risk_level, recorded_at || new Date()]
                );
                
                results.push({ success: true, id: result.insertId });
            } catch (dbError) {
                results.push({ error: dbError.message });
            }
        }
        
        const successful = results.filter(r => r.success).length;
        const failed = results.filter(r => r.error).length;
        
        res.json({
            message: `Upload completed: ${successful} successful, ${failed} failed`,
            results: results
        });
        
    } catch (error) {
        console.error('Error bulk uploading rockfall data:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Start server
app.listen(port, () => {
  console.log(`SEIS AI API server running on port ${port}`);
});