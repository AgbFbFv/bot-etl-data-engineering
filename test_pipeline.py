import pytest
import pandas as pd
from bot_local import ETLPipeline

def test_transform_valid_data():
    pipeline = ETLPipeline('http://dummy', './raw_data', './processed_data', 'test.db')
    mock_data = [
        {'type': 'PushEvent'},
        {'type': 'PushEvent'},
        {'type': 'CreateEvent'}
    ]
    
    df = pipeline.transform(mock_data)
    
    assert 'event_type' in df.columns
    assert 'total_events' in df.columns
    assert len(df) == 2

def test_transform_missing_type_key():
    pipeline = ETLPipeline('http://dummy', './raw_data', './processed_data', 'test.db')
    mock_data = [{'invalid_key': 'value'}]
    
    df = pipeline.transform(mock_data)
    
    assert 'event_type' in df.columns
    assert df['event_type'].iloc[0] == 'UNKNOWN_EVENT'
