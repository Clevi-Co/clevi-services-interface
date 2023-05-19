import sys
import os
sys.path.append(os.getcwd())

from pytest_mock_resources import create_mongo_fixture

mongo_db = create_mongo_fixture()
