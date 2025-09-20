"""Credentials management for database connections."""

import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from ..db.connection import DatabaseConnection
from ..utils.encryption import get_encryption_instance

logger = logging.getLogger(__name__)


@dataclass
class DatabaseCredentials:
    """Database connection credentials."""
    credential_id: int
    connection_id: str
    source_type: str
    host: str
    port: int
    database_name: str
    username: str
    password: str
    ssl_mode: str = 'prefer'
    is_active: bool = True
    description: Optional[str] = None


class CredentialsManager:
    """Manages database credentials storage and retrieval."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize credentials manager.
        
        Args:
            db_connection: Database connection to dsa_production schema
        """
        self.db_connection = db_connection
        self.encryption = get_encryption_instance()
    
    def get_credentials(self, connection_id: str = "test") -> Optional[DatabaseCredentials]:
        """Get credentials for a specific connection ID.
        
        Args:
            connection_id: Connection identifier
            
        Returns:
            DatabaseCredentials object or None if not found
        """
        try:
            with self.db_connection.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT 
                            credential_id,
                            connection_id,
                            source_type,
                            host,
                            port,
                            database_name,
                            username,
                            password_encrypted,
                            ssl_mode,
                            is_active,
                            description
                        FROM dsa_production.credentials
                        WHERE connection_id = %s AND is_active = TRUE
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (connection_id,))
                    
                    result = cur.fetchone()
                    if not result:
                        logger.warning(f"No active credentials found for connection_id: {connection_id}")
                        return None
                    
                    # Decrypt password
                    decrypted_password = self.encryption.decrypt_password(result[7])
                    
                    return DatabaseCredentials(
                        credential_id=result[0],
                        connection_id=result[1],
                        source_type=result[2],
                        host=result[3],
                        port=result[4],
                        database_name=result[5],
                        username=result[6],
                        password=decrypted_password,
                        ssl_mode=result[8],
                        is_active=result[9],
                        description=result[10]
                    )
        
        except Exception as e:
            logger.error(f"Failed to get credentials for {connection_id}: {e}")
            return None
    
    def save_credentials(self, credentials: DatabaseCredentials) -> bool:
        """Save or update credentials.
        
        Args:
            credentials: DatabaseCredentials object to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Encrypt password
            encrypted_password = self.encryption.encrypt_password(credentials.password)
            
            with self.db_connection.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check if credentials already exist
                    cur.execute("""
                        SELECT credential_id FROM dsa_production.credentials
                        WHERE connection_id = %s AND source_type = %s
                    """, (credentials.connection_id, credentials.source_type))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        # Update existing credentials
                        cur.execute("""
                            UPDATE dsa_production.credentials SET
                                host = %s,
                                port = %s,
                                database_name = %s,
                                username = %s,
                                password_encrypted = %s,
                                ssl_mode = %s,
                                is_active = %s,
                                description = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE credential_id = %s
                        """, (
                            credentials.host,
                            credentials.port,
                            credentials.database_name,
                            credentials.username,
                            encrypted_password,
                            credentials.ssl_mode,
                            credentials.is_active,
                            credentials.description,
                            existing[0]
                        ))
                    else:
                        # Insert new credentials
                        cur.execute("""
                            INSERT INTO dsa_production.credentials (
                                connection_id, source_type, host, port, database_name,
                                username, password_encrypted, ssl_mode, is_active, description
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            credentials.connection_id,
                            credentials.source_type,
                            credentials.host,
                            credentials.port,
                            credentials.database_name,
                            credentials.username,
                            encrypted_password,
                            credentials.ssl_mode,
                            credentials.is_active,
                            credentials.description
                        ))
                    
                    conn.commit()
                    logger.info(f"Credentials saved for connection_id: {credentials.connection_id}")
                    return True
        
        except Exception as e:
            logger.error(f"Failed to save credentials: {e}")
            return False
    
    def list_credentials(self) -> List[Dict[str, Any]]:
        """List all stored credentials (without passwords).
        
        Returns:
            List of credential dictionaries
        """
        try:
            with self.db_connection.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            credential_id,
                            connection_id,
                            source_type,
                            host,
                            port,
                            database_name,
                            username,
                            ssl_mode,
                            is_active,
                            description,
                            created_at,
                            updated_at
                        FROM dsa_production.credentials
                        ORDER BY connection_id, created_at DESC
                    """)
                    
                    results = cur.fetchall()
                    credentials_list = []
                    
                    for row in results:
                        credentials_list.append({
                            'credential_id': row[0],
                            'connection_id': row[1],
                            'source_type': row[2],
                            'host': row[3],
                            'port': row[4],
                            'database_name': row[5],
                            'username': row[6],
                            'ssl_mode': row[7],
                            'is_active': row[8],
                            'description': row[9],
                            'created_at': row[10],
                            'updated_at': row[11]
                        })
                    
                    return credentials_list
        
        except Exception as e:
            logger.error(f"Failed to list credentials: {e}")
            return []
    
    def delete_credentials(self, connection_id: str) -> bool:
        """Delete credentials for a connection ID.
        
        Args:
            connection_id: Connection identifier to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_connection.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM dsa_production.credentials
                        WHERE connection_id = %s
                    """, (connection_id,))
                    
                    conn.commit()
                    logger.info(f"Credentials deleted for connection_id: {connection_id}")
                    return True
        
        except Exception as e:
            logger.error(f"Failed to delete credentials: {e}")
            return False
    
    def test_connection(self, credentials: DatabaseCredentials) -> bool:
        """Test database connection with given credentials.
        
        Args:
            credentials: DatabaseCredentials object to test
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Build connection string
            connection_string = f"postgresql://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database_name}"
            
            # Test connection
            test_connection = DatabaseConnection(connection_string)
            return test_connection.test_connection()
        
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
