import pyarrow as pa

# Log schema with timezone-aware timestamp
LOG_SCHEMA = pa.schema([
    ('timestamp', pa.timestamp('us', tz='UTC')),
    ('level', pa.string()),
    ('message', pa.string()),
    ('container', pa.string()),
    ('session', pa.string()),
])
