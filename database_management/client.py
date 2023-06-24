"""
This module provides clients for different types of databases.
You can create a Client instance and then call diferent methods to interact with the database.
"""

import abc
from typing import Literal, List, Optional
import pandas as pd
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from common_utils.testing.check_values import check_none_variables


class SqlClient:
    """
    Abstract class used as a client of a database.
    This is an abstract class. For each database type there is a class that inherits this one.
    """

    def __init__(
        self,
        dialect: str,
        database_name: str,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        driver: Optional[str] = None,
    ) -> None:
        """
        SqlClient Constructor

        Parameters
        ----------
        dialect : str
            database dialect
        database_name : str
            database name
        host : Optional[str], optional
            database host, by default None
        user : Optional[str], optional
            database user, by default None
        password : Optional[str], optional
            database password, by default None
        driver : Optional[str], optional
            database driver, by default None
        """
        self.dialect = dialect
        self.database_name = database_name
        self.host = host
        self.user = user
        self.password = password
        self.driver = driver
        self._engine: Optional[Engine] = None

    @abc.abstractmethod
    def _get_engine(self) -> Engine:
        """
        Obtain a Sqlalchemy Engine to connect to the database.

        Returns
        -------
        Engine
            Slqalchemy Engine class
        """

    def get_session(self) -> Session:
        """
        Obtain a Sqlalchemy Session to interact t the database.

        Returns
        -------
        Session
            Sqlalchemy Session class
        """
        if not self._engine:
            self._engine = self._get_engine()
        session = sessionmaker(self._engine)
        return session()

    def get_dataframe(
        self,
        query: str,
        index_col: Optional[str] = None,
        parse_dates: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Get a dataframe from a table in the database.

        Parameters
        ----------
        query : str
            SQL query to send to the database.
        index_col : Optional[str], optional
            Column to set as index. See Pandas documentation.
        parse_dates : Optional[List[str]], optional
            List of column names to parse as dates. See Pandas documentation.

        Returns
        -------
        pd.DataFrame
            Table obtained as a Pandas dataframe
        """
        if not self._engine:
            self._engine = self._get_engine()
        return pd.read_sql(
            query, self._engine, index_col=index_col, parse_dates=parse_dates
        )

    def insert_dataframe(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        if_exists: Literal["fail", "replace", "append"] = "fail",
        index: bool = False,
    ) -> None:
        """
        Insert a Pandas dataframe into the database.

        Parameters
        ----------
        dataframe : pd.DataFrame
            Pandas dataframe to be inserted into the database
        table_name : str
            Name of the table in the database
        if_exists : Literal["fail", "replace", "append"], optional
            What to do if the table already exists. See Pandas documentation, by default "fail"
        index : bool, optional
            Write the dataframe index as a column. See Pandas documentation, by default False
        """
        if not self._engine:
            self._engine = self._get_engine()
        dataframe.to_sql(table_name, self._engine, if_exists=if_exists, index=index)


class AdvancedClient(SqlClient):
    """
    SqlClient subclass used for databases that have a host, user and password.
    """

    def __init__(
        self,
        dialect: str,
        database_name: str,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        driver: Optional[str] = None,
    ) -> None:
        super().__init__(dialect, database_name, host, user, password, driver)
        check_none_variables(dialect, user, password, host, database_name)
        self.db_uri = f"{dialect}://{user}:{password}@{host}/{database_name}{''if not driver else f'?driver={driver}'}"

    def _get_engine(self) -> Engine:
        if not self._engine:
            self._engine = create_engine(self.db_uri)
        return self._engine


class SqLiteClient(SqlClient):
    """
    SqlClient subclass used for SQLite databases.
    """

    def __init__(
        self,
        dialect: str,
        database_name: str,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        driver: Optional[str] = None,
    ) -> None:
        super().__init__(dialect, database_name, host, user, password, driver)
        check_none_variables(dialect, database_name)
        self.db_uri = f"{dialect}:///{database_name}"

    def _get_engine(self) -> Engine:
        if not self._engine:
            self._engine = create_engine(self.db_uri)
        return self._engine
