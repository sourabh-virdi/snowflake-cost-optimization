# Installation Guide

This guide will walk you through installing and setting up the Snowflake Cost Optimization Platform on your system.

## Prerequisites

### System Requirements
- **Python**: 3.8 or higher
- **Operating System**: Windows, macOS, or Linux
- **Memory**: Minimum 4GB RAM (8GB recommended)
- **Storage**: 1GB free space for installation and cache

### Snowflake Requirements
- **Snowflake Account**: Active Snowflake account with appropriate permissions
- **User Permissions**: Access to `ACCOUNT_USAGE` schema and target databases
- **Authentication**: Username/password or key-pair authentication

## Installation Methods

### Method 1: Local Development Setup (Recommended)

1. **Clone the Repository**
   ```bash
   git clone https://github.com/sourabh-virdi/snowflake-cost-optimization.git
   cd snowflake-cost-optimization
   ```

2. **Create Virtual Environment**
   ```bash
   # Using venv (recommended)
   python -m venv venv
   
   # Activate virtual environment
   # On Windows:
   venv\Scripts\activate
   
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install Dependencies**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Verify Installation**
   ```bash
   python -c "import streamlit; print('Streamlit version:', streamlit.__version__)"
   python -c "import snowflake.connector; print('Snowflake connector installed successfully')"
   ```

### Method 2: Docker Setup

1. **Build Docker Image**
   ```bash
   docker build -t snowflake-optimizer .
   ```

2. **Run Container**
   ```bash
   docker run -p 8501:8501 \
     -e SNOWFLAKE_ACCOUNT=your_account \
     -e SNOWFLAKE_USER=your_user \
     -e SNOWFLAKE_PASSWORD=your_password \
     -e SNOWFLAKE_WAREHOUSE=your_warehouse \
     -e SNOWFLAKE_DATABASE=your_database \
     -e SNOWFLAKE_SCHEMA=your_schema \
     snowflake-optimizer
   ```

### Method 3: pip Install (Future Release)

```bash
# This will be available in future releases
pip install snowflake-cost-optimization
snowflake-optimizer --version
```

## Configuration

### Environment Variables (Recommended for Production)

Create a `.env` file in the project root:

```bash
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role

# Optional: Key-pair authentication
# SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private_key.p8
# SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase

# Application Settings
UI_PAGE_TITLE="Snowflake Cost Optimizer"
CACHE_ENABLED=true
CACHE_TTL_MINUTES=60
LOG_LEVEL=INFO
```

### Configuration File (Alternative)

Edit `config/config.yaml`:

```yaml
snowflake:
  account: your_account_identifier
  user: your_username
  password: your_password
  warehouse: your_warehouse
  database: your_database
  schema: your_schema
  role: your_role

ui:
  page_title: "Snowflake Cost Optimizer"
  page_icon: "SF"
  layout: "wide"
  theme: "light"

cache:
  enabled: true
  default_ttl_minutes: 60
  max_size_mb: 100
  cleanup_interval_minutes: 30

database:
  connection_timeout: 60
  query_timeout: 300
  max_retries: 3
```

## Snowflake Setup

### Required Permissions

Your Snowflake user needs the following permissions:

```sql
-- Grant usage on ACCOUNT_USAGE schema
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE your_role;

-- Grant usage on target warehouse
GRANT USAGE ON WAREHOUSE your_warehouse TO ROLE your_role;

-- Grant usage on target database and schema
GRANT USAGE ON DATABASE your_database TO ROLE your_role;
GRANT USAGE ON SCHEMA your_database.your_schema TO ROLE your_role;

-- Optional: Grant SELECT on specific tables for detailed analysis
GRANT SELECT ON ALL TABLES IN SCHEMA your_database.your_schema TO ROLE your_role;
```

### Key-Pair Authentication Setup (Optional)

1. **Generate Private Key**
   ```bash
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
   ```

2. **Generate Public Key**
   ```bash
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   ```

3. **Configure Snowflake User**
   ```sql
   ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';
   ```

4. **Update Configuration**
   ```bash
   SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8
   SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase
   ```

## Running the Application

### Local Development

```bash
# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Run the application
streamlit run streamlit_app/main.py

# Alternative: Using Python module
python -m streamlit run streamlit_app/main.py

# With custom port
streamlit run streamlit_app/main.py --server.port 8502
```

### Production Deployment

```bash
# Run with production settings
streamlit run streamlit_app/main.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  --server.enableCORS false \
  --server.enableXsrfProtection true
```

### Docker Deployment

```bash
# Using Docker Compose
docker-compose up -d

# Or direct Docker run
docker run -d \
  --name snowflake-optimizer \
  -p 8501:8501 \
  --env-file .env \
  snowflake-optimizer:latest
```

## Verification

### Test Connection

1. **Open Browser**: Navigate to `http://localhost:8501`
2. **Test Connection**: Click "Connect" in the sidebar
3. **Verify Data**: Check that dashboards load with your Snowflake data

### Run Tests

```bash
# Run unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=html

# Run integration tests (requires Snowflake connection)
pytest tests/integration/ -v -m "not snowflake"
```

## Troubleshooting

### Common Issues

#### Connection Failed
```
Error: Connection failed: HTTP 403: Forbidden
```
**Solution**: Check account identifier and user permissions

#### Missing Dependencies
```
ModuleNotFoundError: No module named 'snowflake.connector'
```
**Solution**: Ensure virtual environment is activated and dependencies installed

#### Permission Denied
```
SQL compilation error: Insufficient privileges to operate on database 'SNOWFLAKE'
```
**Solution**: Grant `IMPORTED PRIVILEGES` on `SNOWFLAKE` database

#### Cache Issues
```
PermissionError: [Errno 13] Permission denied: 'query_cache'
```
**Solution**: Ensure write permissions in project directory

### Debug Mode

Enable debug logging:

```bash
# Set environment variable
export LOG_LEVEL=DEBUG

# Or edit config.yaml
logging:
  level: DEBUG
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

### Performance Issues

If the application is slow:

1. **Check Cache Settings**
   ```python
   # Increase cache TTL
   cache:
     default_ttl_minutes: 120
   ```

2. **Optimize Query Timeouts**
   ```python
   database:
     query_timeout: 600  # 10 minutes
   ```

3. **Monitor Resource Usage**
   ```bash
   # Check memory usage
   docker stats snowflake-optimizer
   
   # Check disk usage
   du -sh query_cache/
   ```

## Next Steps

1. **Configuration**: [Complete your configuration](configuration.md)
2. **First Steps**: [Take your first steps with the platform](first-steps.md)
3. **User Guide**: [Explore the dashboard features](../user-guide/dashboard.md)
4. **API Reference**: [Learn about the API](../api/connectors.md)

## Getting Help

- **Documentation**: Check our [comprehensive documentation](../index.md)
- **GitHub Issues**: Report bugs or request features
- **Community**: Join our discussions and share experiences
- **Support**: Contact our team for enterprise support

---

**Installation complete!** You're ready to start optimizing your Snowflake costs. Continue to the [Configuration Guide](configuration.md) to customize your setup. 