# Environment Segregation and Data Separation Setup Guide

## Overview
This guide explains how to set up GA AlignOps with proper development and production environment separation, including data segregation and the restructured module organization.

## 🏗️ New Module Structure

### **Demand Planning** 📋
- **Target Setting**: Sales target configuration
- **Demand Tweaking**: ML-powered demand forecasting
- **Editable Plan View**: Interactive demand planning
- **Sales Dashboard**: Real-time sales metrics
- **Forecasting Engine**: Advanced ML forecasting

### **Supply Management** 🔄
- **Talent Management**: Talent pool management
- **Staffing Planning**: Position and resource planning
- **Demand-Supply Mapping**: *Moved from separate module*
- **Resource Allocation**: Skills-based allocation
- **Skills Matrix**: Talent capability mapping

### **Billing Management** 💰
- **Planned Billing**: Billing planning and forecasting
- **Actual Billing**: Real billing tracking
- **Billing Reconciliation**: Planned vs actual comparison
- **Invoice Management**: Invoice automation
- **Financial Reports**: Financial analytics

### **Insights & Analytics** 📊
- **Analytics Dashboard**: Business intelligence
- **Performance Monitor**: System performance tracking
- **Business Intelligence**: Advanced reporting
- **Trend Analysis**: Data trend insights
- **Custom Reports**: Personalized reporting

### **Settings** ⚙️
- **User Management**: User administration
- **Role Management**: Permission control
- **Environment Settings**: Environment configuration
- **Google Sheets Config**: Integration settings
- **OAuth Settings**: Authentication configuration

## 🔧 Environment Setup

### 1. Development Environment

#### Database Configuration
```bash
# Development uses same database but with dev_ prefixed tables
DATABASE_URL=postgresql://username:password@localhost:5432/gaalignops_dev
GAALIGNOPS_ENV=development
```

#### Table Structure
- **Production tables**: `unified_sales_data`, `master_clients`, etc.
- **Development tables**: `dev_unified_sales_data`, `dev_master_clients`, etc.
- **Data isolation**: Development tables are completely separate
- **Auto-sync**: Development tables automatically sync from production structure

#### Features
- ✅ OAuth disabled (fallback authentication)
- ✅ Google Sheets sync enabled
- ✅ Scheduler enabled (non-background)
- ✅ Debug mode enabled
- ✅ Data validation disabled
- ✅ Production data protection disabled

### 2. Production Environment

#### Database Configuration
```bash
# Production uses clean table names
DATABASE_URL=postgresql://username:password@production-host:5432/gaalignops_prod
GAALIGNOPS_ENV=production
```

#### Table Structure
- **Clean table names**: `unified_sales_data`, `master_clients`, etc.
- **No prefixes**: Direct access to production data
- **Data protection**: Production data protection enabled
- **Validation**: Full data validation enabled

#### Features
- ✅ OAuth enabled (Google authentication)
- ✅ Google Sheets sync enabled
- ✅ Scheduler enabled (background mode)
- ✅ Debug mode disabled
- ✅ Data validation enabled
- ✅ Production data protection enabled

## 📊 Data Segregation

### Development Environment
```sql
-- Development tables are automatically created with dev_ prefix
CREATE TABLE dev_unified_sales_data (LIKE unified_sales_data INCLUDING ALL);
CREATE TABLE dev_master_clients (LIKE master_clients INCLUDING ALL);
CREATE TABLE dev_talent_supply (LIKE talent_supply INCLUDING ALL);

-- Data is copied from production for testing
INSERT INTO dev_unified_sales_data SELECT * FROM unified_sales_data;
INSERT INTO dev_master_clients SELECT * FROM master_clients;
INSERT INTO dev_talent_supply SELECT * FROM talent_supply;
```

### Production Environment
```sql
-- Production uses original table names
SELECT * FROM unified_sales_data;
SELECT * FROM master_clients;
SELECT * FROM talent_supply;
```

### Data Sync Process
1. **Development startup**: Automatically creates dev_ tables
2. **Structure copy**: Copies table structure from production
3. **Data sync**: Copies production data to development tables
4. **Isolation**: Development changes never affect production
5. **Refresh**: Can manually sync fresh production data

## 🚀 Setup Instructions

### Step 1: Environment Configuration

#### For Development
```bash
# Copy development environment file
cp config/development.env .env

# Edit .env with your local database details
DATABASE_URL=postgresql://username:password@localhost:5432/gaalignops_dev
GAALIGNOPS_ENV=development
```

#### For Production
```bash
# Copy production environment file
cp config/production.env .env

# Edit .env with your production details
DATABASE_URL=postgresql://username:password@production-host:5432/gaalignops_prod
GAALIGNOPS_ENV=production
GOOGLE_CLIENT_ID=your_production_client_id
GOOGLE_CLIENT_SECRET=your_production_client_secret
```

### Step 2: Database Setup

#### Local Development Database
```bash
# Create local PostgreSQL database
createdb gaalignops_dev

# The application will automatically create dev_ tables on startup
```

#### Production Database
```bash
# Ensure production database exists
# The application will use existing production tables
```

### Step 3: Google Sheets Integration

#### Development
- Use development Google Sheets
- Data validation disabled
- Sync schedule: 8 PM IST daily

#### Production
- Use production Google Sheets
- Data validation enabled
- Sync schedule: 8 PM IST daily
- Background scheduler enabled

### Step 4: OAuth Configuration

#### Development
- OAuth disabled
- Fallback authentication enabled
- SSL not required

#### Production
- Google OAuth enabled
- Domain restriction: greyamp.com only
- SSL required
- Fallback authentication disabled

## 🔄 Data Import from Replit

### What You Need to Import

#### 1. Database Data
```bash
# Export from Replit production database
pg_dump $DATABASE_URL > replit_production_backup.sql

# Import to your local development database
psql $DATABASE_URL < replit_production_backup.sql
```

#### 2. Environment Variables
```bash
# Copy these from Replit Secrets to your .env file
GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET
DATABASE_URL
```

#### 3. Google Sheets Configuration
- Copy Google Sheets credentials
- Update sheet IDs for your environment
- Configure sync schedules

### Import Process

#### Step 1: Export from Replit
1. Access your Replit project
2. Go to Secrets management
3. Copy all environment variables
4. Export database if needed

#### Step 2: Local Setup
1. Create local database
2. Import production data
3. Configure environment files
4. Test application startup

#### Step 3: Production Setup
1. Deploy to production environment
2. Configure production database
3. Set production environment variables
4. Enable OAuth and production features

## 🧪 Testing Environment Separation

### Development Testing
```python
# Check environment
from config.environments import EnvironmentManager
env_manager = EnvironmentManager()

print(f"Environment: {env_manager.environment}")
print(f"Table prefix: {env_manager.get_config().table_prefix}")
print(f"OAuth enabled: {env_manager.get_config().oauth_config['enabled']}")

# Should output:
# Environment: development
# Table prefix: dev_
# OAuth enabled: False
```

### Production Testing
```python
# Check environment
from config.environments import EnvironmentManager
env_manager = EnvironmentManager()

print(f"Environment: {env_manager.environment}")
print(f"Table prefix: {env_manager.get_config().table_prefix}")
print(f"OAuth enabled: {env_manager.get_config().oauth_config['enabled']}")

# Should output:
# Environment: production
# Table prefix: 
# OAuth enabled: True
```

## 🔒 Security Features

### Development
- Fallback authentication
- Debug information visible
- Data validation disabled
- Production data protection disabled

### Production
- Google OAuth required
- Domain restriction enforced
- SSL required
- Production data protection enabled
- Session timeout enforced

## 📋 Checklist

### Development Setup
- [ ] Copy `development.env` to `.env`
- [ ] Configure local database URL
- [ ] Set `GAALIGNOPS_ENV=development`
- [ ] Test application startup
- [ ] Verify dev_ tables created
- [ ] Test fallback authentication

### Production Setup
- [ ] Copy `production.env` to `.env`
- [ ] Configure production database URL
- [ ] Set `GAALIGNOPS_ENV=production`
- [ ] Configure Google OAuth credentials
- [ ] Test OAuth authentication
- [ ] Verify production tables accessible

### Data Import
- [ ] Export data from Replit
- [ ] Import to local development database
- [ ] Test data sync functionality
- [ ] Verify module restructuring
- [ ] Test all new module features

## 🆘 Troubleshooting

### Common Issues

#### 1. Environment Not Detected
```bash
# Check environment variable
echo $GAALIGNOPS_ENV

# Should be 'development' or 'production'
```

#### 2. Development Tables Not Created
```python
# Check environment manager
from config.environments import EnvironmentManager
env_manager = EnvironmentManager()
env_manager._ensure_development_tables()
```

#### 3. OAuth Not Working
```bash
# Check OAuth configuration
echo $GOOGLE_CLIENT_ID
echo $GOOGLE_CLIENT_SECRET
echo $GAALIGNOPS_ENV
```

#### 4. Module Navigation Issues
```python
# Check module structure
from config.module_structure import module_structure
print(module_structure.get_module_names())
```

## 📞 Support

For issues with environment setup:
1. Check environment variables
2. Verify database connectivity
3. Check application logs
4. Test environment detection
5. Verify table creation

The restructured application now provides:
- ✅ Complete environment separation
- ✅ Data isolation between dev and prod
- ✅ Consolidated module structure
- ✅ Enhanced billing management
- ✅ Improved supply management
- ✅ Production-ready OAuth
- ✅ Automated data sync
