import unittest
import StringIO
import pandas
import tempfile
import os.path

from databass.interpretor import PullBasedInterpretor
from databass.optimizer import Optimizer
from databass.ops import Limit
from databass.db import Database
from databass.parse_sql import parse 

import pandas.util.testing as pdt

db = Database()
opt = Optimizer(db)
interp = PullBasedInterpretor(db)



class TestUnits(unittest.TestCase):
  """Basic unit testing"""

  def queries_not_equal(self, q1, q2):
    ast1 = parse(q1)
    ast2 = parse(q2)
    self.assertNotEqual(str(ast1), str(ast2))

  def run_query(self, q):
    ast = parse(q)
    plan = opt(ast)
    return [row for row in interp(plan)]

  def test_parse(self):
    """""" 
    parse("SELECT 1 FROM data LIMIT 1 OFFSET 1")
    parse("SELECT 1 FROM data LIMIT 1 OFFSET 1+1")

    with self.assertRaises(Exception):
      parse("SELECT 1 FROM data LIMIT 1 OFFSET")

    with self.assertRaises(Exception):
      parse("SELECT 1 FROM data OFFSET 1 LIMIT 1")

    with self.assertRaises(Exception):
      parse("SELECT a FROM data LIMIT 1 OFFSET -1")

  def test_tostr(self):
    self.queries_not_equal(
        "SELECT 1 FROM data LIMIT 1",
        "SELECT 1 FROM data LIMIT 1 OFFSET 1")

  def test_constantoffset(self):
    truth = [{'a': 1}, {'a': 2}, {'a': 3}]
    results = self.run_query("SELECT a FROM data LIMIT 3 OFFSET 1")
    self.assertEqual(results, truth)



if __name__ == '__main__':
  unittest.main()
