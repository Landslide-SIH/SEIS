require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const mysql = require('mysql2/promise');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { trainRiskModel, predictRisk } = require('./ml-model');

const app = express();
const port = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key';

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// Serve static files
app.use(express.static(__dirname));

// Database configuration
const dbConfig = {
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'devbansal',
  password: process.env.DB_PASSWORD || 'Jb1525@jb',
  database: process.env.DB_NAME || 'seis_ai',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  charset: 'utf8mb4'
};

let pool;

// Initialize database connection pool
async function initializePool() {
  try {
    pool = mysql.createPool(dbConfig);
    
    // Test connection
    const connection = await pool.getConnection();
    console.log('Connected to MySQL database');
    connection.release();
    
    // Initialize database tables
    await initDatabase();
  } catch (error) {
    console.error('Error connecting to MySQL:', error.message);
    // Retry connection after delay
    setTimeout(initializePool, 5000);
  }
}

// Initialize database tables
async function initDatabase() {
  const connection = await pool.getConnection();
  
  try {
    // Read and execute SQL file
    const fs = require('fs');
    const path = require('path');
    const sql = fs.readFileSync(path.join(__dirname, 'database.sql'), 'utf8');
    
    // Split into individual statements
    const statements = sql.split(';').filter(statement => statement.trim() !== '');
    
    for (const statement of statements) {
      if (statement.trim() !== '') {
        try {
          await connection.execute(statement + ';');
          console.log('Executed SQL statement');
        } catch (error) {
          // Ignore "table already exists" errors
          if (!error.message.includes('already exists')) {
            console.error('Error executing statement:', error.message);
          }
        }
      }
    }
    
    console.log('Database tables initialized');
  } catch (error) {
    console.error('Error initializing database:', error.message);
  } finally {
    connection.release();
  }
}

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

// Input validation middleware
const validateMineData = (req, res, next) => {
  const { id, name, region, lat, lng } = req.body;
  
  if (!id || !name || !region) {
    return res.status(400).json({ error: 'ID, name, and region are required' });
  }
  
  if (lat && (lat < -90 || lat > 90)) {
    return res.status(400).json({ error: 'Latitude must be between -90 and 90' });
  }
  
  if (lng && (lng < -180 || lng > 180)) {
    return res.status(400).json({ error: 'Longitude must be between -180 and 180' });
  }
  
  next();
};

// API Routes

// Health check endpoint
app.get('/api/health', async (req, res) => {
  try {
    await pool.execute('SELECT 1');
    res.json({ status: 'OK', database: 'Connected' });
  } catch (error) {
    res.status(500).json({ status: 'Error', database: 'Disconnected', error: error.message });
  }
});

// Get all mines
app.get('/api/mines', authenticateToken, async (req, res) => {
  try {
    const [rows] = await pool.execute(`
      SELECT m.*, 
        (SELECT risk_level FROM predictions WHERE mine_id = m.id ORDER BY timestamp DESC LIMIT 1) as risk_level,
        (SELECT risk_score FROM predictions WHERE mine_id = m.id ORDER BY timestamp DESC LIMIT 1) as risk_score,
        (SELECT timestamp FROM predictions WHERE mine_id = m.id ORDER BY timestamp DESC LIMIT 1) as prediction_timestamp
      FROM mines m
      ORDER BY m.name
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
    
    // Get latest prediction
    const [predictionRows] = await pool.execute(
      'SELECT * FROM predictions WHERE mine_id = ? ORDER BY timestamp DESC LIMIT 1',
      [id]
    );
    
    // Get prediction history (last 7 days)
    const [predictionHistory] = await pool.execute(
      'SELECT risk_score, timestamp FROM predictions WHERE mine_id = ? AND timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY) ORDER BY timestamp',
      [id]
    );
    
    // Get sensors
    const [sensorRows] = await pool.execute(
      'SELECT * FROM sensors WHERE mine_id = ? ORDER BY sensor_type',
      [id]
    );
    
    // Get latest sensor readings
    const sensorReadings = [];
    for (const sensor of sensorRows) {
      const [readings] = await pool.execute(
        'SELECT * FROM sensor_readings WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT 10',
        [sensor.id]
      );
      sensorReadings.push(...readings);
    }
    
    // Get alerts
    const [alertRows] = await pool.execute(
      'SELECT * FROM alerts WHERE mine_id = ? ORDER BY timestamp DESC LIMIT 10',
      [id]
    );
    
    // Get weather data
    const [weatherRows] = await pool.execute(
      'SELECT * FROM weather_data WHERE mine_id = ? ORDER BY recorded_at DESC LIMIT 24',
      [id]
    );
    
    res.json({
      mine: mineRows[0],
      prediction: predictionRows[0] || null,
      prediction_history: predictionHistory,
      sensors: sensorRows,
      sensor_readings: sensorReadings,
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
    
    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ error: 'Invalid email format' });
    }
    
    // Check password strength
    if (password.length < 8) {
      return res.status(400).json({ error: 'Password must be at least 8 characters long' });
    }
    
    // Check if user already exists
    const [existingUsers] = await pool.execute('SELECT id FROM users WHERE email = ?', [email]);
    
    if (existingUsers.length > 0) {
      return res.status(409).json({ error: 'User already exists' });
    }
    
    // Hash password
    const hashedPassword = await bcrypt.hash(password, 12);
    
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

// Additional endpoints would follow the same pattern with improved error handling

// Start server
initializePool().then(() => {
  app.listen(port, () => {
    console.log(`SEIS AI API server running on port ${port}`);
  });
}).catch(error => {
  console.error('Failed to initialize database connection:', error);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  if (pool) {
    await pool.end();
  }
  process.exit(0);
});