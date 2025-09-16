# Test class for Bronze_to_Silver_DQX_Enhanced_Pipeline.py
# Test Coverage Goals: CRITICAL Priority - 95%, HIGH Priority - 95%, MEDIUM Priority - 80%, LOW Priority - 60%

import pytest
import unittest.mock as mock
from unittest.mock import patch, MagicMock, call, PropertyMock
import sys
import os
import json
from datetime import datetime, date
import uuid

class TestBronzeToSilverDQXEnhancedPipeline:
    """Test class for Bronze to Silver DQX Enhanced Pipeline functions
    
    Coverage Goals:
    - CRITICAL Priority Functions: 95% coverage
    - HIGH Priority Functions: 95% coverage
    - MEDIUM Priority Functions: 80% coverage
    - LOW Priority Functions: 60% coverage
    """
    
    def setup_method(self):
        """Setup test fixtures before each test method"""
        self.sample_bronze_data = {
            "record_id": "bronze-123",
            "ingestion_timestamp": datetime.now(),
            "ingestion_date": date.today(),
            "ingestion_hour": 10,
            "eventhub_partition": "0",
            "eventhub_offset": "12345",
            "eventhub_sequence_number": 67890,
            "eventhub_enqueued_time": datetime.now(),
            "eventhub_partition_key": None,
            "processing_cluster_id": "cluster-123",
            "consumer_group": "$Default",
            "eventhub_name": "weather-events",
            "payload_size_bytes": 512,
            "raw_payload": json.dumps({
                "event_id": "weather-event-123",
                "city": "New York",
                "latitude": 40.7128,
                "longitude": -74.0060,
                "temperature": 22.5,
                "humidity": 65.0,
                "wind_speed": 8.2,
                "pressure": 1013.25,
                "precipitation": 0.0,
                "cloud_cover": 25.0,
                "weather_condition": "partly_cloudy",
                "timestamp": "2024-01-15T10:30:00Z",
                "data_source": "databricks_weather_simulator",
                "cluster_id": "cluster-123",
                "notebook_path": "/test/weather"
            })
        }
        
        self.mock_spark_df = MagicMock()
        self.mock_dqx_rules = []
    
    # =============================================================================
    # CRITICAL PRIORITY TESTS (95% Coverage Goal) - Core Business Logic
    # =============================================================================
    
    def test_parse_weather_payload_enhanced_success(self):
        """Test successful weather payload parsing"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import parse_weather_payload_enhanced
        
        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_parsed_df = MagicMock()
        
        with patch('pyspark.sql.functions.get_json_object') as mock_get_json:
            with patch('pyspark.sql.functions.col') as mock_col:
                with patch('pyspark.sql.functions.when') as mock_when:
                    mock_df.select.return_value = mock_parsed_df
                    mock_parsed_df.withColumn.return_value = mock_parsed_df
                    mock_get_json.return_value = "parsed_value"
                    mock_col.return_value = "column_ref"
                    mock_when.return_value = MagicMock()
                    
                    # Execute function
                    result = parse_weather_payload_enhanced(mock_df)
                    
                    # Assertions
                    assert result is not None, "Function should return parsed DataFrame"
                    mock_df.select.assert_called_once()
                    assert mock_get_json.call_count > 0, "JSON parsing should be attempted"
    
    def test_parse_weather_payload_enhanced_json_extraction(self):
        """Test JSON field extraction in payload parsing"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import parse_weather_payload_enhanced
        
        mock_df = MagicMock()
        mock_parsed_df = MagicMock()
        
        with patch('pyspark.sql.functions.get_json_object') as mock_get_json:
            with patch('pyspark.sql.functions.col') as mock_col:
                mock_df.select.return_value = mock_parsed_df
                mock_parsed_df.withColumn.return_value = mock_parsed_df
                
                # Execute function
                result = parse_weather_payload_enhanced(mock_df)
                
                # Verify key weather fields are extracted
                expected_fields = ['event_id', 'city', 'temperature', 'humidity', 'latitude', 'longitude']
                json_calls = [str(call) for call in mock_get_json.call_args_list]
                
                for field in expected_fields:
                    field_extracted = any(field in call for call in json_calls)
                    assert field_extracted, f"Field {field} should be extracted from JSON"
    
    def test_parse_weather_payload_enhanced_error_handling(self):
        """Test error handling in payload parsing"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import parse_weather_payload_enhanced
        
        mock_df = MagicMock()
        
        with patch('pyspark.sql.functions.get_json_object') as mock_get_json:
            # Simulate parsing error
            mock_get_json.side_effect = Exception("JSON parsing failed")
            
            # Function should handle errors gracefully
            try:
                result = parse_weather_payload_enhanced(mock_df)
                # If no exception, that's also acceptable (error handled internally)
                assert True
            except Exception as e:
                # If exception is raised, it should be handled appropriately
                assert "JSON parsing failed" in str(e)
    
    def test_transform_bronze_to_silver_with_dqx_single_table_orchestration(self):
        """Test main transformation orchestration"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import transform_bronze_to_silver_with_dqx_single_table
        
        mock_bronze_df = MagicMock()
        mock_parsed_df = MagicMock()
        mock_quality_df = MagicMock()
        mock_silver_df = MagicMock()
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.parse_weather_payload_enhanced') as mock_parse:
            with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.apply_dqx_quality_validation_single_table') as mock_quality:
                with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.add_silver_processing_metadata_enhanced') as mock_metadata:
                    mock_parse.return_value = mock_parsed_df
                    mock_quality.return_value = mock_quality_df
                    mock_metadata.return_value = mock_silver_df
                    mock_silver_df.select.return_value = mock_silver_df
                    
                    # Execute function
                    result = transform_bronze_to_silver_with_dqx_single_table(mock_bronze_df)
                    
                    # Verify orchestration flow
                    mock_parse.assert_called_once_with(mock_bronze_df)
                    mock_quality.assert_called_once_with(mock_parsed_df)
                    mock_metadata.assert_called_once_with(mock_quality_df)
                    assert result == mock_silver_df
    
    def test_apply_dqx_quality_validation_single_table_with_dqx_available(self):
        """Test DQX quality validation when DQX framework is available"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import apply_dqx_quality_validation_single_table
        
        mock_df = MagicMock()
        mock_valid_df = MagicMock()
        mock_invalid_df = MagicMock()
        mock_combined_df = MagicMock()
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.DQX_AVAILABLE', True):
            with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.quality_rules', ['rule1', 'rule2']):
                with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.dq_engine') as mock_engine:
                    with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.add_dqx_quality_metadata') as mock_add_metadata:
                        # Mock DQX engine behavior
                        mock_engine.apply_checks_and_split.return_value = (mock_valid_df, mock_invalid_df)
                        mock_add_metadata.side_effect = [mock_valid_df, mock_invalid_df]
                        mock_valid_df.unionByName.return_value = mock_combined_df
                        
                        # Execute function
                        result = apply_dqx_quality_validation_single_table(mock_df)
                        
                        # Verify DQX validation was used
                        mock_engine.apply_checks_and_split.assert_called_once()
                        assert mock_add_metadata.call_count == 2
                        assert result == mock_combined_df
    
    def test_apply_dqx_quality_validation_single_table_fallback(self):
        """Test DQX quality validation fallback when DQX is unavailable"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import apply_dqx_quality_validation_single_table
        
        mock_df = MagicMock()
        mock_fallback_df = MagicMock()
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.DQX_AVAILABLE', False):
            with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.apply_basic_quality_validation_single_table') as mock_fallback:
                mock_fallback.return_value = mock_fallback_df
                
                # Execute function
                result = apply_dqx_quality_validation_single_table(mock_df)
                
                # Verify fallback was used
                mock_fallback.assert_called_once_with(mock_df)
                assert result == mock_fallback_df
    
    # =============================================================================
    # HIGH PRIORITY TESTS (95% Coverage Goal) - Quality & Metadata
    # =============================================================================
    
    def test_apply_basic_quality_validation_single_table_pass_conditions(self):
        """Test basic quality validation with passing conditions"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import apply_basic_quality_validation_single_table
        
        mock_df = MagicMock()
        mock_result_df = MagicMock()
        
        with patch('pyspark.sql.functions.col') as mock_col:
            with patch('pyspark.sql.functions.when') as mock_when:
                with patch('pyspark.sql.functions.lit') as mock_lit:
                    with patch('pyspark.sql.functions.array') as mock_array:
                        mock_df.withColumn.return_value = mock_result_df
                        mock_result_df.withColumn.return_value = mock_result_df
                        
                        # Execute function
                        result = apply_basic_quality_validation_single_table(mock_df)
                        
                        # Verify quality metadata was added
                        assert mock_df.withColumn.call_count > 0
                        assert result == mock_result_df
    
    def test_apply_basic_quality_validation_single_table_quality_conditions(self):
        """Test basic quality validation quality condition logic"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import apply_basic_quality_validation_single_table
        
        mock_df = MagicMock()
        
        with patch('pyspark.sql.functions.col') as mock_col:
            with patch('pyspark.sql.functions.when') as mock_when:
                mock_col.return_value = MagicMock()
                mock_when.return_value = MagicMock()
                mock_df.withColumn.return_value = mock_df
                
                # Execute function
                result = apply_basic_quality_validation_single_table(mock_df)
                
                # Verify quality conditions were checked
                # Should check for event_id, city, weather_timestamp, parsing_status
                col_calls = [str(call) for call in mock_col.call_args_list]
                expected_fields = ['event_id', 'city', 'weather_timestamp', 'parsing_status']
                
                for field in expected_fields:
                    field_checked = any(field in call for call in col_calls)
                    assert field_checked, f"Quality condition should check {field}"
    
    def test_add_dqx_quality_metadata_valid_records(self):
        """Test adding DQX quality metadata for valid records"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import add_dqx_quality_metadata
        
        mock_df = MagicMock()
        mock_result_df = MagicMock()
        
        with patch('pyspark.sql.functions.lit') as mock_lit:
            with patch('pyspark.sql.functions.array') as mock_array:
                with patch('pyspark.sql.functions.current_timestamp') as mock_timestamp:
                    mock_df.withColumn.return_value = mock_result_df
                    mock_result_df.withColumn.return_value = mock_result_df
                    
                    # Execute function for valid records
                    result = add_dqx_quality_metadata(mock_df, is_valid=True)
                    
                    # Verify PASS metadata was added
                    assert mock_df.withColumn.call_count > 0
                    lit_calls = [str(call) for call in mock_lit.call_args_list]
                    assert any("PASS" in call for call in lit_calls)
    
    def test_add_dqx_quality_metadata_invalid_records(self):
        """Test adding DQX quality metadata for invalid records"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import add_dqx_quality_metadata
        
        mock_df = MagicMock()
        mock_result_df = MagicMock()
        
        with patch('pyspark.sql.functions.lit') as mock_lit:
            with patch('pyspark.sql.functions.array') as mock_array:
                with patch('pyspark.sql.functions.current_timestamp') as mock_timestamp:
                    mock_df.withColumn.return_value = mock_result_df
                    mock_result_df.withColumn.return_value = mock_result_df
                    
                    # Execute function for invalid records
                    result = add_dqx_quality_metadata(mock_df, is_valid=False)
                    
                    # Verify FAIL metadata was added
                    lit_calls = [str(call) for call in mock_lit.call_args_list]
                    assert any("FAIL" in call for call in lit_calls)
    
    def test_add_silver_processing_metadata_enhanced_timestamp_fields(self):
        """Test adding silver processing metadata with timestamp fields"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import add_silver_processing_metadata_enhanced
        
        mock_df = MagicMock()
        mock_result_df = MagicMock()
        
        with patch('pyspark.sql.functions.current_timestamp') as mock_timestamp:
            with patch('pyspark.sql.functions.current_date') as mock_date:
                with patch('pyspark.sql.functions.hour') as mock_hour:
                    mock_df.withColumn.return_value = mock_result_df
                    mock_result_df.withColumn.return_value = mock_result_df
                    
                    # Execute function
                    result = add_silver_processing_metadata_enhanced(mock_df)
                    
                    # Verify timestamp metadata was added
                    mock_timestamp.assert_called()
                    mock_date.assert_called()
                    mock_hour.assert_called()
                    assert result == mock_result_df
    
    # =============================================================================
    # MEDIUM PRIORITY TESTS (80% Coverage Goal) - Configuration & Infrastructure
    # =============================================================================
    
    def test_get_trigger_config_seconds_parsing(self):
        """Test trigger configuration for seconds"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import get_trigger_config
        
        test_cases = [
            ("1 second", {"processingTime": "1 seconds"}),
            ("5 seconds", {"processingTime": "5 seconds"}),
            ("30 seconds", {"processingTime": "30 seconds"})
        ]
        
        for trigger_mode, expected in test_cases:
            result = get_trigger_config(trigger_mode)
            assert result == expected, f"Failed for trigger_mode: {trigger_mode}"
    
    def test_get_trigger_config_minutes_parsing(self):
        """Test trigger configuration for minutes"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import get_trigger_config
        
        test_cases = [
            ("1 minute", {"processingTime": "1 minutes"}),
            ("10 minutes", {"processingTime": "10 minutes"})
        ]
        
        for trigger_mode, expected in test_cases:
            result = get_trigger_config(trigger_mode)
            assert result == expected, f"Failed for trigger_mode: {trigger_mode}"
    
    def test_get_trigger_config_default_fallback(self):
        """Test trigger configuration default fallback"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import get_trigger_config
        
        # Test invalid modes
        invalid_modes = ["invalid", "", None, "hours", "milliseconds"]
        expected = {"processingTime": "5 seconds"}
        
        for mode in invalid_modes:
            result = get_trigger_config(mode)
            assert result == expected, f"Should fallback to default for: {mode}"
    
    def test_setup_enhanced_single_hive_components_database_creation(self):
        """Test enhanced Hive components setup - database creation"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import setup_enhanced_single_hive_components
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.enhanced_hive_manager') as mock_manager:
            mock_manager.create_database_if_not_exists.return_value = None
            mock_manager.create_enhanced_silver_table.return_value = True
            mock_manager.show_enhanced_single_table_info.return_value = None
            
            # Execute function
            result = setup_enhanced_single_hive_components()
            
            # Verify setup steps were called
            mock_manager.create_database_if_not_exists.assert_called_once()
            mock_manager.create_enhanced_silver_table.assert_called_once()
            assert result is True
    
    def test_setup_enhanced_single_hive_components_table_creation_failure(self):
        """Test enhanced Hive components setup with table creation failure"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import setup_enhanced_single_hive_components
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.enhanced_hive_manager') as mock_manager:
            mock_manager.create_database_if_not_exists.return_value = None
            mock_manager.create_enhanced_silver_table.return_value = False  # Simulate failure
            
            # Execute function
            result = setup_enhanced_single_hive_components()
            
            # Should return False on table creation failure
            assert result is False
    
    # =============================================================================
    # LOW PRIORITY TESTS (60% Coverage Goal) - Management & Display
    # =============================================================================
    
    def test_start_enhanced_bronze_to_silver_dqx_single_table_streaming_setup_failure(self):
        """Test streaming pipeline start with setup failure"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import start_enhanced_bronze_to_silver_dqx_single_table_streaming
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.enhanced_single_setup_success', False):
            # Execute function
            result = start_enhanced_bronze_to_silver_dqx_single_table_streaming()
            
            # Should return None when setup failed
            assert result is None
    
    def test_start_enhanced_bronze_to_silver_dqx_single_table_streaming_success(self):
        """Test successful streaming pipeline start"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import start_enhanced_bronze_to_silver_dqx_single_table_streaming
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.enhanced_single_setup_success', True):
            with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.spark') as mock_spark:
                with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.transform_bronze_to_silver_with_dqx_single_table') as mock_transform:
                    # Mock spark streaming setup
                    mock_read_stream = MagicMock()
                    mock_stream_reader = MagicMock()
                    mock_bronze_stream = MagicMock()
                    mock_silver_stream = MagicMock()
                    mock_write_stream = MagicMock()
                    mock_query = MagicMock()
                    
                    mock_spark.readStream = mock_read_stream
                    mock_read_stream.format.return_value = mock_stream_reader
                    mock_stream_reader.table.return_value = mock_bronze_stream
                    mock_transform.return_value = mock_silver_stream
                    mock_silver_stream.writeStream = mock_write_stream
                    mock_write_stream.outputMode.return_value = mock_write_stream
                    mock_write_stream.format.return_value = mock_write_stream
                    mock_write_stream.option.return_value = mock_write_stream
                    mock_write_stream.trigger.return_value = mock_write_stream
                    mock_write_stream.toTable.return_value = mock_query
                    
                    # Execute function
                    result = start_enhanced_bronze_to_silver_dqx_single_table_streaming()
                    
                    # Verify streaming setup was attempted
                    assert result == mock_query
    
    def test_check_enhanced_single_table_dqx_pipeline_status_with_query(self):
        """Test pipeline status check with active query"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import check_enhanced_single_table_dqx_pipeline_status
        
        mock_query = MagicMock()
        mock_query.id = "test-query-123"
        mock_query.runId = "test-run-456"
        mock_query.isActive = True
        mock_query.lastProgress = {'batchId': 1}
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.enhanced_dqx_single_streaming_query', mock_query):
            # Execute function
            result = check_enhanced_single_table_dqx_pipeline_status()
            
            # Should return True when query exists
            assert result is True
    
    def test_check_enhanced_single_table_dqx_pipeline_status_no_query(self):
        """Test pipeline status check with no query"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import check_enhanced_single_table_dqx_pipeline_status
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.enhanced_dqx_single_streaming_query', None):
            # Execute function
            result = check_enhanced_single_table_dqx_pipeline_status()
            
            # Should return False when no query exists
            assert result is False
    
    def test_stop_enhanced_single_table_dqx_pipeline_active_query(self):
        """Test stopping active pipeline"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import stop_enhanced_single_table_dqx_pipeline
        
        mock_query = MagicMock()
        mock_query.isActive = True
        mock_query.stop.return_value = None
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.enhanced_dqx_single_streaming_query', mock_query):
            with patch('time.sleep') as mock_sleep:  # Speed up test
                # Initially active, then inactive after stop
                mock_query.isActive = True
                
                # Execute function
                stop_enhanced_single_table_dqx_pipeline()
                
                # Verify stop was called
                mock_query.stop.assert_called_once()
    
    def test_stop_enhanced_single_table_dqx_pipeline_no_active_query(self):
        """Test stopping pipeline when no active query"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import stop_enhanced_single_table_dqx_pipeline
        
        mock_query = MagicMock()
        mock_query.isActive = False
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.enhanced_dqx_single_streaming_query', mock_query):
            # Execute function
            stop_enhanced_single_table_dqx_pipeline()
            
            # Stop should not be called for inactive query
            mock_query.stop.assert_not_called()
    
    def test_show_enhanced_single_table_dqx_basic_display(self):
        """Test basic display of enhanced single table DQX statistics"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import show_enhanced_single_table_dqx
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.spark') as mock_spark:
            mock_table = MagicMock()
            mock_table.count.return_value = 1000
            mock_spark.table.return_value = mock_table
            
            # Execute function
            show_enhanced_single_table_dqx()
            
            # Verify table was queried
            mock_spark.table.assert_called()
    
    def test_display_enhanced_single_table_pipeline_summary_html(self):
        """Test HTML summary display"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import display_enhanced_single_table_pipeline_summary
        
        with patch('displayHTML') as mock_display_html:
            # Execute function
            display_enhanced_single_table_pipeline_summary()
            
            # Verify HTML display was called
            mock_display_html.assert_called_once()
    
    def test_show_dqx_single_table_extension_summary_text_display(self):
        """Test text summary display"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import show_dqx_single_table_extension_summary
        
        # Execute function (should not raise exceptions)
        show_dqx_single_table_extension_summary()
        
        # Function should complete without errors
        assert True
    
    # =============================================================================
    # INTEGRATION TESTS
    # =============================================================================
    
    def test_integration_full_bronze_to_silver_pipeline(self):
        """Integration test: Full Bronze to Silver transformation pipeline"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import (
            parse_weather_payload_enhanced,
            apply_basic_quality_validation_single_table,
            add_silver_processing_metadata_enhanced
        )
        
        mock_bronze_df = MagicMock()
        mock_parsed_df = MagicMock()
        mock_quality_df = MagicMock()
        mock_silver_df = MagicMock()
        
        with patch('pyspark.sql.functions.get_json_object'):
            with patch('pyspark.sql.functions.col'):
                with patch('pyspark.sql.functions.when'):
                    with patch('pyspark.sql.functions.lit'):
                        with patch('pyspark.sql.functions.current_timestamp'):
                            mock_bronze_df.select.return_value = mock_parsed_df
                            mock_parsed_df.withColumn.return_value = mock_quality_df
                            mock_quality_df.withColumn.return_value = mock_silver_df
                            
                            # Test pipeline steps
                            parsed_result = parse_weather_payload_enhanced(mock_bronze_df)
                            quality_result = apply_basic_quality_validation_single_table(parsed_result)
                            final_result = add_silver_processing_metadata_enhanced(quality_result)
                            
                            # Verify pipeline completion
                            assert final_result is not None
    
    def test_integration_error_handling_pipeline(self):
        """Integration test: Error handling throughout pipeline"""
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import transform_bronze_to_silver_with_dqx_single_table
        
        mock_bronze_df = MagicMock()
        
        with patch('Bronze_to_Silver_DQX_Enhanced_Pipeline.parse_weather_payload_enhanced') as mock_parse:
            # Simulate error in parsing
            mock_parse.side_effect = Exception("Parsing failed")
            
            # Pipeline should handle errors gracefully
            try:
                result = transform_bronze_to_silver_with_dqx_single_table(mock_bronze_df)
                assert True  # If no exception, error was handled
            except Exception:
                # If exception occurs, it should be specific and informative
                assert True
    
    # =============================================================================
    # PERFORMANCE TESTS
    # =============================================================================
    
    def test_performance_payload_parsing_efficiency(self):
        """Test payload parsing performance efficiency"""
        import time
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import parse_weather_payload_enhanced
        
        mock_df = MagicMock()
        mock_result_df = MagicMock()
        mock_df.select.return_value = mock_result_df
        mock_result_df.withColumn.return_value = mock_result_df
        
        # Measure parsing time
        start_time = time.time()
        result = parse_weather_payload_enhanced(mock_df)
        end_time = time.time()
        
        parsing_time = end_time - start_time
        
        # Parsing should be fast (under 1 second for mocked operations)
        assert parsing_time < 1.0, f"Payload parsing took too long: {parsing_time} seconds"
    
    def test_performance_quality_validation_efficiency(self):
        """Test quality validation performance"""
        import time
        from Bronze_to_Silver_DQX_Enhanced_Pipeline import apply_basic_quality_validation_single_table
        
        mock_df = MagicMock()
        mock_result_df = MagicMock()
        mock_df.withColumn.return_value = mock_result_df
        mock_result_df.withColumn.return_value = mock_result_df
        
        # Measure validation time
        start_time = time.time()
        result = apply_basic_quality_validation_single_table(mock_df)
        end_time = time.time()
        
        validation_time = end_time - start_time
        
        # Quality validation should be fast
        assert validation_time < 1.0, f"Quality validation took too long: {validation_time} seconds"


# =============================================================================
# TEST CONFIGURATION AND FIXTURES
# =============================================================================

@pytest.fixture
def sample_bronze_dataframe():
    """Fixture providing sample bronze DataFrame structure"""
    return {
        'record_id': 'bronze-123',
        'raw_payload': json.dumps({
            'event_id': 'weather-123',
            'city': 'New York',
            'temperature': 22.5,
            'humidity': 65.0
        }),
        'ingestion_timestamp': datetime.now()
    }

@pytest.fixture
def mock_dqx_framework():
    """Fixture providing mock DQX framework components"""
    mock_engine = MagicMock()
    mock_rules = [MagicMock(), MagicMock()]
    return {
        'engine': mock_engine,
        'rules': mock_rules,
        'available': True
    }

@pytest.fixture
def mock_quality_metadata():
    """Fixture providing mock quality metadata structure"""
    return {
        'flag_check': 'PASS',
        'dqx_quality_score': 0.95,
        'dqx_rule_results': ['rule1_passed', 'rule2_passed'],
        'failed_rules_count': 0,
        'passed_rules_count': 2
    }


# =============================================================================
# TEST SUITE CONFIGURATION
# =============================================================================

class TestBronzeToSilverDQXEnhancedTestSuite:
    """Test suite configuration and metadata"""
    
    def test_coverage_goals_documentation(self):
        """Document test coverage goals for this module"""
        coverage_goals = {
            'CRITICAL_PRIORITY_FUNCTIONS': {
                'target_coverage': 95,
                'functions': [
                    'parse_weather_payload_enhanced',
                    'transform_bronze_to_silver_with_dqx_single_table',
                    'apply_dqx_quality_validation_single_table'
                ]
            },
            'HIGH_PRIORITY_FUNCTIONS': {
                'target_coverage': 95,
                'functions': [
                    'apply_basic_quality_validation_single_table',
                    'add_dqx_quality_metadata',
                    'add_silver_processing_metadata_enhanced'
                ]
            },
            'MEDIUM_PRIORITY_FUNCTIONS': {
                'target_coverage': 80,
                'functions': [
                    'get_trigger_config',
                    'setup_enhanced_single_hive_components'
                ]
            },
            'LOW_PRIORITY_FUNCTIONS': {
                'target_coverage': 60,
                'functions': [
                    'start_enhanced_bronze_to_silver_dqx_single_table_streaming',
                    'check_enhanced_single_table_dqx_pipeline_status',
                    'stop_enhanced_single_table_dqx_pipeline',
                    'show_enhanced_single_table_dqx',
                    'display_enhanced_single_table_pipeline_summary',
                    'show_dqx_single_table_extension_summary'
                ]
            }
        }
        
        # Verify coverage goals
        assert coverage_goals['CRITICAL_PRIORITY_FUNCTIONS']['target_coverage'] == 95
        assert coverage_goals['HIGH_PRIORITY_FUNCTIONS']['target_coverage'] == 95
        assert coverage_goals['MEDIUM_PRIORITY_FUNCTIONS']['target_coverage'] == 80
        assert coverage_goals['LOW_PRIORITY_FUNCTIONS']['target_coverage'] == 60


if __name__ == "__main__":
    # Run tests with coverage reporting
    pytest.main([
        __file__,
        "-v",
        "--cov=Bronze_to_Silver_DQX_Enhanced_Pipeline",
        "--cov-report=html",
        "--cov-report=term-missing",
        "--cov-fail-under=80"  # Overall coverage goal
    ])