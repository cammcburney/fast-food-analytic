import pytest
import pandas as pd
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.testing.assertions import AssertsExecutionResults
from sqlalchemy.exc import SQLAlchemyError
from unittest.mock import MagicMock, patch
from src.utils.processing_utils import *

class Test_Run_Query_With_Engine:
    def test_one(self):
        assert 1==2
    

    