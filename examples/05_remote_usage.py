from perceptra_storage import get_storage_adapter

# Configuration
config = {
    'base_url': 'https://storage.yourcompany.com',
    'timeout': 30,
    'verify_ssl': True,
    'max_retries': 3
}

# Credentials (choose one method)
credentials = {
    'api_key': 'your-api-key'  # API Key auth
}
# OR
credentials = {
    'token': 'your-bearer-token'  # Bearer token auth
}
# OR
credentials = {
    'username': 'user',
    'password': 'pass'  # Basic auth
}

# Create adapter
adapter = get_storage_adapter('remote', config, credentials)

# Test connection
adapter.test_connection()

# Use like other adapters
with open('file.txt', 'rb') as f:
    adapter.upload_file(f, 'documents/file.txt')