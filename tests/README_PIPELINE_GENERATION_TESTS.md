# Pipeline Generation Test Suite Documentation

## Overview

This test suite provides comprehensive validation of the staffing plan pipeline generation functionality without modifying any core logic. The tests ensure that both new and edit modes work correctly and that data persists properly.

## CRITICAL POLICY

**DO NOT MODIFY THE PIPELINE GENERATION FUNCTIONALITY** unless explicitly requested by the user. These tests exist to validate that the existing functionality works correctly and should be used as a safety net before any changes.

## Test Files

### 1. `test_staffing_plan_pipeline_generation.py`
**Core functionality tests focusing on:**
- Pipeline backwards calculation accuracy
- Database persistence in new and edit modes
- Data integrity validation
- Session state management
- Error handling

**Key Test Cases:**
- `test_02_pipeline_backwards_calculation` - Validates the core calculation logic
- `test_03_new_staffing_plan_creation` - Tests new plan pipeline generation
- `test_04_edit_mode_pipeline_generation` - Tests edit mode functionality
- `test_05_pipeline_calculation_accuracy` - Verifies calculation precision
- `test_06_data_persistence_validation` - Confirms database save/load

### 2. `test_pipeline_generation_integration.py`
**Integration tests focusing on:**
- UI component interaction
- Session state flow between new and edit modes
- Auto-restore functionality
- Form clearing behavior differences
- Performance validation

**Key Test Cases:**
- `test_01_new_mode_session_state_flow` - New plan session state management
- `test_02_edit_mode_session_state_flow` - Edit mode session state management
- `test_03_auto_restore_functionality` - Tests the auto-restore fix
- `test_04_form_clearing_behavior` - Validates clearing differences between modes
- `test_05_pipeline_calculation_integration` - End-to-end integration testing

### 3. `run_pipeline_generation_tests.py`
**Test runner providing:**
- Comprehensive test execution
- Detailed reporting
- Environment validation
- Performance monitoring
- Failed test analysis

## Running the Tests

### Full Test Suite
```bash
python3 tests/run_pipeline_generation_tests.py
```

### Individual Test Files
```bash
# Core functionality tests
python3 -m pytest tests/test_staffing_plan_pipeline_generation.py -v

# Integration tests
python3 -m pytest tests/test_pipeline_generation_integration.py -v
```

### Specific Test Functions
```bash
python3 tests/run_pipeline_generation_tests.py test_02_pipeline_backwards_calculation
```

## Integration with Build Pipeline

The tests are integrated into the build pipeline (`build_pipeline.sh`) as Step 6. They will:
- Run automatically during builds
- Fail the build if pipeline generation is broken
- Provide detailed error reports
- Ensure deployment safety

## Test Data Management

**IMPORTANT:** All tests use the `INTEGRATION_TEST` and `TEST` prefixes for data to avoid conflicts with real data. Test data is automatically cleaned up before and after test execution.

### Test Data Isolation
- Uses development environment only (`dev_` prefixed tables)
- Creates isolated test pipelines and plans
- Automatic cleanup prevents data pollution
- No impact on production or user data

## What the Tests Validate

### Core Pipeline Logic
- Backwards calculation from target hires to initial stage
- Conversion rate application across all stages
- TAT (Turn Around Time) calculations
- Special stage handling (terminal, success stages)
- Date-based pipeline scheduling

### Data Flow
- Session state management in new vs edit modes
- Database save and load operations
- JSON serialization/deserialization
- Form data persistence
- Auto-restore functionality

### UI Integration
- Form clearing behavior differences
- Session state restoration
- Error handling and recovery
- Performance under load
- Data consistency across components

## Expected Behavior

### New Mode
- Form clears after successful save
- Session state resets
- Fresh pipeline generation each time
- Clean form state for next plan

### Edit Mode
- Form data persists after save
- Pipeline data remains visible
- Session state maintained
- Continuous editing capability
- Auto-restore if session lost

## Troubleshooting

### Test Failures
1. Check environment (must be development)
2. Verify database connectivity
3. Review test data cleanup
4. Check for concurrent test runs

### Common Issues
- **Environment Error**: Tests must run in development mode
- **Database Connection**: Verify DATABASE_URL is set
- **Permission Issues**: Ensure test file permissions are correct
- **Data Conflicts**: May need manual cleanup if tests interrupted

## Maintenance

### Adding New Tests
1. Follow existing naming conventions
2. Use `INTEGRATION_TEST` or `TEST` prefixes for test data
3. Implement proper cleanup in setup/teardown
4. Document the test purpose and validation

### Updating Tests
1. **NEVER modify core functionality without user request**
2. Update tests to reflect any approved changes
3. Maintain backward compatibility
4. Document changes in test comments

## Success Criteria

All tests should pass before any deployment. The tests validate:
- ✅ Pipeline backwards calculation works correctly
- ✅ New mode creates and saves plans properly
- ✅ Edit mode loads and updates plans correctly
- ✅ Session state behaves differently in new vs edit modes
- ✅ Auto-restore functionality works when session is lost
- ✅ Data persists correctly in database
- ✅ Error handling works for edge cases
- ✅ Performance meets acceptable standards

## Contact

If tests fail or you need to modify pipeline generation functionality, please:
1. Review the test output carefully
2. Identify the specific failing test
3. Understand what the test validates
4. Get explicit approval before making any changes to core logic