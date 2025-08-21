# Cascading Pipeline Integration & Status Flag Behavior Tests

## Overview

This document describes the comprehensive test suite that protects the critical cascading dropdown logic and status flag behavior in the pipeline system. **These behaviors MUST NOT CHANGE** as they form the core functionality of the workforce intelligence platform.

## Test Coverage

### 1. Cascading Pipeline Integration Tests (`test_cascading_pipeline_integration.py`)

Tests the critical Client → Pipeline → Plan Owner → Role cascading logic:

#### Key Test Cases:
- **Client to Pipeline Cascading**: Selecting a client shows only their pipelines
- **Pipeline to Plan Owner Cascading**: Selecting a pipeline shows only owners from its staffing plans  
- **Plan Owner to Role Cascading**: Selecting an owner shows only their roles
- **Cross-Client Isolation**: Data properly isolated between different clients
- **Complete Workflow Testing**: End-to-end cascading behavior validation
- **Data Consistency**: Validates relationships across the entire cascading system

#### Critical Business Logic Protected:
```sql
-- Client A should only see their pipelines
SELECT id, name FROM dev_talent_pipelines 
WHERE client_id = %s AND name LIKE '%CASCADE_TEST%'

-- Pipeline should only show owners from its staffing plans
SELECT DISTINCT ppd.plan_owner 
FROM dev_pipeline_planning_details ppd
JOIN dev_staffing_plans sp ON ppd.plan_id = sp.id
WHERE sp.pipeline_id = %s

-- Owner should only show their roles
SELECT DISTINCT ppd.role 
FROM dev_pipeline_planning_details ppd
WHERE ppd.plan_owner = %s
```

### 2. Status Flag Pipeline Behavior Tests (`test_status_flag_pipeline_behavior.py`)

Tests the critical status flag behavior that determines candidate counting in pipeline calculations:

#### Status Flag Types:
- **Greyamp**: Count only in Greyamp pipeline calculations
- **Client**: Count only in Client pipeline calculations  
- **Both**: Count in BOTH Greyamp AND Client calculations

#### Key Test Cases:
- **Greyamp-Only Status Counting**: Verifies Greyamp-managed statuses only appear in Greyamp calculations
- **Client-Only Status Counting**: Verifies Client-managed statuses only appear in Client calculations
- **Both Status Double Counting**: Verifies Both-managed statuses appear in BOTH calculations
- **Pipeline Calculation Views**: Tests both Greyamp and Client perspective calculations
- **Actual Count Accuracy**: Validates "Actual #" calculations respect status flags
- **Comprehensive Integration**: Tests complete integration with pipeline system

#### Critical Status Flag Behavior:
```sql
-- Greyamp calculation (includes Greyamp + Both)
SELECT cd.status, COUNT(*) as count
FROM dev_candidate_data cd
JOIN dev_pipeline_stages ps ON (cd.status = ps.stage_name)
WHERE ps.status_flag IN ('Greyamp', 'Both')

-- Client calculation (includes Client + Both)  
SELECT cd.status, COUNT(*) as count
FROM dev_candidate_data cd
JOIN dev_pipeline_stages ps ON (cd.status = ps.stage_name)
WHERE ps.status_flag IN ('Client', 'Both')
```

## Test Data Structure

### Realistic Test Scenarios:
- **Multiple Clients**: CASCADE_TEST_ClientA, CASCADE_TEST_ClientB
- **Multiple Pipelines per Client**: Different pipeline configurations
- **Multiple Owners per Pipeline**: Different plan owners with distinct roles
- **Comprehensive Status Coverage**: All status flag combinations (Greyamp, Client, Both)
- **Real Candidate Data**: Candidates with various statuses mapped to different pipeline stages

### Status Flag Examples:
```python
# Greyamp-managed stages
('Applied', 1, 10, 'Greyamp'),
('Screening', 2, 70, 'Greyamp'),
('Tech Round', 3, 80, 'Greyamp'),

# Client-managed stages  
('Client Interview', 5, 90, 'Client'),
('Selected', 6, 95, 'Client'),
('On Boarded', 7, 100, 'Client'),

# Both-managed stages (critical!)
('Dropped', 0, 0, 'Both'),
('Sourced', 1, 15, 'Both')
```

## Integration with Build Pipeline

Both test suites are integrated into the build pipeline via:

1. **`build_pipeline.sh`**: Runs tests as separate steps
2. **`run_tests.py`**: Includes tests in comprehensive test runner

### Build Pipeline Integration:
```bash
# Step 5.1: Run Cascading Pipeline Integration Tests
pytest tests/test_cascading_pipeline_integration.py -v

# Step 5.2: Run Status Flag Pipeline Behavior Tests  
pytest tests/test_status_flag_pipeline_behavior.py -v
```

## Critical Protection Points

### 1. Cascading Logic Protection:
- Client selection MUST determine available pipelines
- Pipeline selection MUST determine available plan owners
- Plan owner selection MUST determine available roles
- Cross-client data MUST remain isolated

### 2. Status Flag Behavior Protection:
- "Dropped" status MUST have "Both" flag (appears in both calculations)
- Greyamp-only statuses MUST NOT appear in Client calculations
- Client-only statuses MUST NOT appear in Greyamp calculations
- Both-managed statuses MUST appear in BOTH calculations

### 3. Data Consistency Protection:
- All candidates MUST have valid client-pipeline-owner-role relationships
- No orphaned records allowed
- Status-to-pipeline-stage mappings MUST be consistent

## Running Tests

### Individual Test Execution:
```bash
# Run cascading tests only
pytest tests/test_cascading_pipeline_integration.py -v

# Run status flag tests only  
pytest tests/test_status_flag_pipeline_behavior.py -v

# Run both new test suites
pytest tests/test_cascading_pipeline_integration.py tests/test_status_flag_pipeline_behavior.py -v
```

### Full Build Pipeline:
```bash
# Run complete build pipeline (includes all tests)
./build_pipeline.sh
```

## Test Environment Requirements

- **Environment**: Development only (`dev_` prefixed tables)
- **Database**: PostgreSQL with development data
- **Dependencies**: pytest, psycopg2-binary
- **Cleanup**: Automatic test data cleanup before and after tests

## Failure Prevention

These tests will **FAIL THE BUILD** if:
- Cascading dropdown logic is broken
- Status flag behavior is modified incorrectly
- Cross-client data isolation is compromised
- Pipeline calculation logic changes unexpectedly
- Data consistency requirements are violated

## Maintenance Notes

- **NEVER** modify the core cascading logic without updating tests
- **NEVER** change status flag behavior without verifying test coverage
- **ALWAYS** run these tests before deploying pipeline-related changes
- **ALWAYS** update test data if new status types are added

These tests ensure the stability and reliability of the core pipeline functionality that users depend on for accurate workforce planning and pipeline management.