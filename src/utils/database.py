"""
Database Connection and Operations Module
Regional Economics Database for NRW

Handles database connections, sessions, and common database operations.
"""

from contextlib import contextmanager
from typing import Optional, Generator, Any, Dict, List
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool, QueuePool
from sqlalchemy.engine import Engine

from .config import get_config
from .logging import get_logger


logger = get_logger(__name__)


class DatabaseManager:
    """Manages database connections and operations."""

    def __init__(self, db_name: str = 'regional_economics'):
        """
        Initialize the database manager.

        Args:
            db_name: Name of the database configuration to use
        """
        self.db_name = db_name
        self.config = get_config()
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._metadata: Optional[MetaData] = None

    def _create_engine(self) -> Engine:
        """
        Create SQLAlchemy engine.

        Returns:
            SQLAlchemy Engine instance
        """
        connection_string = self.config.get_db_connection_string(self.db_name)
        db_config = self.config.database

        # Get SQLAlchemy settings
        sqlalchemy_config = db_config.get('sqlalchemy', {})
        pool_config = db_config.get('connection_pool', {})

        engine = create_engine(
            connection_string,
            echo=sqlalchemy_config.get('echo', False),
            pool_pre_ping=sqlalchemy_config.get('pool_pre_ping', True),
            pool_recycle=sqlalchemy_config.get('pool_recycle', 3600),
            pool_size=pool_config.get('max_size', 10),
            max_overflow=pool_config.get('max_size', 10) // 2,
            poolclass=QueuePool
        )

        logger.info(f"Database engine created for: {self.db_name}")
        return engine

    @property
    def engine(self) -> Engine:
        """Get or create database engine."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """Get or create session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.

        Yields:
            SQLAlchemy Session instance

        Example:
            with db.get_session() as session:
                result = session.execute(text("SELECT 1"))
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()

    @contextmanager
    def get_connection(self):
        """
        Context manager for raw database connections.

        Yields:
            SQLAlchemy Connection instance

        Example:
            with db.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
        """
        connection = self.engine.connect()
        try:
            yield connection
            connection.commit()
        except Exception as e:
            connection.rollback()
            logger.error(f"Connection error: {e}")
            raise
        finally:
            connection.close()

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            List of dictionaries containing query results
        """
        with self.get_connection() as conn:
            if params:
                result = conn.execute(text(query), params)
            else:
                result = conn.execute(text(query))

            # Convert to list of dicts
            rows = []
            for row in result:
                rows.append(dict(row._mapping))

            logger.debug(f"Query executed, {len(rows)} rows returned")
            return rows

    def execute_statement(self, statement: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE statement.

        Args:
            statement: SQL statement string
            params: Optional statement parameters

        Returns:
            Number of rows affected
        """
        with self.get_connection() as conn:
            if params:
                result = conn.execute(text(statement), params)
            else:
                result = conn.execute(text(statement))

            rows_affected = result.rowcount
            logger.debug(f"Statement executed, {rows_affected} rows affected")
            return rows_affected

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()

            logger.info("Database connection test successful")
            return True

        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def get_table_metadata(self, table_name: str) -> Optional[Table]:
        """
        Get metadata for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            SQLAlchemy Table object or None if not found
        """
        if self._metadata is None:
            self._metadata = MetaData()
            self._metadata.reflect(bind=self.engine)

        return self._metadata.tables.get(table_name)

    def get_table_names(self) -> List[str]:
        """
        Get list of all table names in the database.

        Returns:
            List of table names
        """
        if self._metadata is None:
            self._metadata = MetaData()
            self._metadata.reflect(bind=self.engine)

        return list(self._metadata.tables.keys())

    def bulk_insert(self, table_name: str, records: List[Dict[str, Any]]) -> int:
        """
        Bulk insert records into a table.

        Args:
            table_name: Name of the target table
            records: List of dictionaries containing record data

        Returns:
            Number of records inserted
        """
        if not records:
            logger.warning("No records to insert")
            return 0

        table = self.get_table_metadata(table_name)
        if table is None:
            raise ValueError(f"Table not found: {table_name}")

        with self.get_connection() as conn:
            result = conn.execute(table.insert(), records)
            count = result.rowcount

        logger.info(f"Bulk inserted {count} records into {table_name}")
        return count

    def close(self) -> None:
        """Close database connections and dispose of engine."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            logger.info("Database connections closed")


# Global database manager instances
_db_instances: Dict[str, DatabaseManager] = {}


def get_database(db_name: str = 'regional_economics') -> DatabaseManager:
    """
    Get database manager instance (singleton per database).

    Args:
        db_name: Name of the database configuration

    Returns:
        DatabaseManager instance
    """
    global _db_instances

    if db_name not in _db_instances:
        _db_instances[db_name] = DatabaseManager(db_name)

    return _db_instances[db_name]
