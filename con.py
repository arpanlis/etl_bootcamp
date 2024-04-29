from typing import Optional
from snowflake.snowpark import Session

DEFAULT_CONNECTION_PARAMETERS = {
    "account": "ZA08283.ap-south-1.aws",
    "user": "ARPANATLIS",
    "password": "M5UG*6ghu6!rrn",
    "database": "BHATBHATENI",
}


def get_session(
    account: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
) -> Session:
    connection_parameters = DEFAULT_CONNECTION_PARAMETERS.copy()

    if account:
        connection_parameters["account"] = account

    if user:
        connection_parameters["user"] = user

    if password:
        connection_parameters["password"] = password

    if database:
        connection_parameters["database"] = database

    if schema:
        connection_parameters["schema"] = schema

    return Session.builder.configs(connection_parameters).create()  # type: ignore
