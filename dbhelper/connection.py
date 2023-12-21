from __future__ import annotations
from sqlalchemy.engine import create_engine, Engine, URL, make_url


def create_connection(url: str) -> Engine:
    """
    Create a connection to the specified URL and return an Engine object.

    Parameters:
        url (str): The URL to connect to.

    Returns:
        Engine: The Engine object representing the connection.
    """
    u: URL = make_url(url)
    return create_engine(u)
