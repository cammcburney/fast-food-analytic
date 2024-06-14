import pytest
import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.testing.assertions import AssertsExecutionResults
from unittest.mock import MagicMock, patch
from src.utils.load_utils import run_engine_to_insert_database

class TestEngineInsertData:
    pass