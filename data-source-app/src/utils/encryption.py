"""Encryption utilities for secure password storage."""

import base64
import os
import json
from pathlib import Path
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class KeyStorage:
    """Handles persistent storage of encryption keys."""
    
    def __init__(self, key_file: str = None):
        """Initialize key storage.
        
        Args:
            key_file: Path to key file. If None, uses default location.
        """
        if key_file is None:
            # Store key in user's home directory under .dsa directory
            home_dir = Path.home()
            dsa_dir = home_dir / '.dsa'
            dsa_dir.mkdir(exist_ok=True)
            self.key_file = dsa_dir / 'encryption_key.json'
        else:
            self.key_file = Path(key_file)
    
    def get_key(self) -> str:
        """Get stored encryption key.
        
        Returns:
            Encryption key string
        """
        if not self.key_file.exists():
            return None
        
        try:
            with open(self.key_file, 'r') as f:
                data = json.load(f)
                return data.get('key')
        except (json.JSONDecodeError, KeyError, IOError):
            return None
    
    def store_key(self, key: str) -> bool:
        """Store encryption key.
        
        Args:
            key: Encryption key to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            self.key_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Store key with metadata
            data = {
                'key': key,
                'created_at': str(Path().cwd()),  # Store current working directory as context
                'version': '1.0'
            }
            
            with open(self.key_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Set restrictive permissions (readable only by owner)
            os.chmod(self.key_file, 0o600)
            return True
        except (IOError, OSError):
            return False
    
    def key_exists(self) -> bool:
        """Check if key file exists.
        
        Returns:
            True if key file exists, False otherwise
        """
        return self.key_file.exists()
    
    def delete_key(self) -> bool:
        """Delete stored key.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.key_file.exists():
                self.key_file.unlink()
            return True
        except OSError:
            return False


class PasswordEncryption:
    """Handles encryption and decryption of passwords for secure storage."""
    
    def __init__(self, master_key: str = None):
        """Initialize encryption with a master key.
        
        Args:
            master_key: Master key for encryption. If None, uses stored key or generates a new one.
        """
        self.key_storage = KeyStorage()
        
        if master_key is None:
            # Try to get key from environment variable first
            master_key = os.getenv('DSA_MASTER_KEY')
            
            # If not in environment, try to get from stored key
            if master_key is None:
                master_key = self.key_storage.get_key()
            
            # If still no key, generate a new one and store it
            if master_key is None:
                master_key = Fernet.generate_key().decode()
                if self.key_storage.store_key(master_key):
                    print(f"✅ Generated and stored new encryption key")
                else:
                    print(f"⚠️  Generated new key but failed to store it. Set DSA_MASTER_KEY={master_key} for consistency.")
        
        self.master_key = master_key.encode() if isinstance(master_key, str) else master_key
        self._fernet = None
    
    def _get_fernet(self) -> Fernet:
        """Get or create Fernet instance for encryption/decryption."""
        if self._fernet is None:
            # Derive key from master key using PBKDF2
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'dsa_salt_2024',  # Fixed salt for consistency
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(self.master_key))
            self._fernet = Fernet(key)
        return self._fernet
    
    def encrypt_password(self, password: str) -> str:
        """Encrypt a password for secure storage.
        
        Args:
            password: Plain text password to encrypt
            
        Returns:
            Base64 encoded encrypted password
        """
        if not password:
            return ""
        
        fernet = self._get_fernet()
        encrypted_bytes = fernet.encrypt(password.encode())
        return base64.urlsafe_b64encode(encrypted_bytes).decode()
    
    def decrypt_password(self, encrypted_password: str) -> str:
        """Decrypt a password from storage.
        
        Args:
            encrypted_password: Base64 encoded encrypted password
            
        Returns:
            Decrypted plain text password
        """
        if not encrypted_password or encrypted_password == "PLACEHOLDER_ENCRYPTED_PASSWORD":
            return ""
        
        try:
            fernet = self._get_fernet()
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_password.encode())
            decrypted_bytes = fernet.decrypt(encrypted_bytes)
            return decrypted_bytes.decode()
        except Exception as e:
            raise ValueError(f"Failed to decrypt password: {e}")
    
    def is_encrypted(self, password: str) -> bool:
        """Check if a password is already encrypted.
        
        Args:
            password: Password string to check
            
        Returns:
            True if password appears to be encrypted
        """
        if not password:
            return False
        
        try:
            # Try to decode as base64
            base64.urlsafe_b64decode(password.encode())
            return True
        except Exception:
            return False
    
    def store_current_key(self) -> bool:
        """Store the current master key.
        
        Returns:
            True if successful, False otherwise
        """
        key_str = self.master_key.decode() if isinstance(self.master_key, bytes) else self.master_key
        return self.key_storage.store_key(key_str)
    
    def get_stored_key_info(self) -> dict:
        """Get information about the stored key.
        
        Returns:
            Dictionary with key information or None if no key stored
        """
        if not self.key_storage.key_exists():
            return None
        
        try:
            with open(self.key_storage.key_file, 'r') as f:
                data = json.load(f)
                return {
                    'key_file': str(self.key_storage.key_file),
                    'created_at': data.get('created_at', 'Unknown'),
                    'version': data.get('version', 'Unknown'),
                    'key_exists': True
                }
        except (json.JSONDecodeError, KeyError, IOError):
            return None
    
    def delete_stored_key(self) -> bool:
        """Delete the stored key.
        
        Returns:
            True if successful, False otherwise
        """
        return self.key_storage.delete_key()


def get_encryption_instance() -> PasswordEncryption:
    """Get a shared encryption instance."""
    return PasswordEncryption()
