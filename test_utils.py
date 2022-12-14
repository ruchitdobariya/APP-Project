from mock_db import MockDB
from mock import patch
import utils

class TestUtils(MockDB):

    def test_db_write(self):
        with self.mock_db_config:
            self.assertEqual(utils.db_write("""INSERT INTO branch (branch_id, branchname, address) VALUES
                            ('1', 'branch1', 'abc')"""), True)
            self.assertEqual(utils.db_write("""DELETE FROM branch WHERE branch_id='1' """), True)
            self.assertEqual(utils.db_write("""INSERT INTO branch (branch_id, branchname, address) VALUES
                            ('2', 'branch2', 'def)"""), False)
            self.assertEqual(utils.db_write("""DELETE FROM branch WHERE branch_id='2' """), True)

            print("Testing Done")