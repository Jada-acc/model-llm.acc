import pytest
from datetime import datetime, timedelta
import time
from .auth_manager import AuthManager
from .encryption import DataEncryption

@pytest.fixture
def auth_manager():
    return AuthManager("test_secret_key")

@pytest.fixture
def encryption():
    return DataEncryption()

def test_token_generation(auth_manager):
    """Test JWT token generation and validation."""
    token = auth_manager.generate_token("test_user")
    assert token is not None
    
    payload = auth_manager.validate_token(token)
    assert payload is not None
    assert payload['user_id'] == "test_user"

def test_token_expiration(auth_manager):
    """Test token expiration."""
    token = auth_manager.generate_token("test_user", expires_in=1)
    assert token is not None
    
    # Wait for token to expire
    time.sleep(2)
    payload = auth_manager.validate_token(token)
    assert payload is None

def test_token_revocation(auth_manager):
    """Test token revocation."""
    token = auth_manager.generate_token("test_user")
    assert token is not None
    
    # Revoke token
    assert auth_manager.revoke_token("test_user") is True
    
    # Try to validate revoked token
    payload = auth_manager.validate_token(token)
    assert payload is None

def test_data_encryption(encryption):
    """Test data encryption and decryption."""
    test_data = {
        "sensitive": "information",
        "numbers": [1, 2, 3],
        "nested": {"data": "value"}
    }
    
    # Encrypt data
    encrypted = encryption.encrypt_data(test_data)
    assert encrypted is not None
    
    # Decrypt data
    decrypted = encryption.decrypt_data(encrypted)
    assert decrypted == test_data

def test_key_rotation(encryption):
    """Test encryption key rotation."""
    test_data = {"before": "rotation"}
    
    # Encrypt with original key
    encrypted = encryption.encrypt_data(test_data)
    assert encrypted is not None
    
    # Rotate key
    assert encryption.rotate_key() is True
    
    # Try to decrypt with new key (should fail)
    decrypted = encryption.decrypt_data(encrypted)
    assert decrypted is None 