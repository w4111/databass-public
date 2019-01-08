from exprs import *
from compiler import *
import unittest
from db import Database
#from parse_sql import parse
import random
import timeit

	#This test checks that you have correctly implemented the operators. 
  #Remember that you need to update both the __call__ and compile() methods to make this work.
class TestUnits(unittest.TestCase):

    def test_exprs(self):
    
        # <= test
        exp1 = Expr("<=", Const(10), Const(2))
        # Interpreted evaluation
        assert exp1(None) == False
        # Compiled evaluation
        expr = exp1.compile()
        assert eval(expr) == False
        
        # >= test
        exp2 = Expr(">=", Const(10), Const(2))
        # Interpreted evaluation
        assert exp2(None) == True
        # Compiled evaluation
        expr = exp2.compile()
        assert eval(expr) == True
        
        # != test
        exp3 = Expr("!=", Const(10), Const(2))
        # Interpreted evaluation
        assert exp3(None) == True
        # Compiled evaluation
        expr = exp3.compile()
        assert eval(expr) == True
        
        # - test
        exp4 = Expr("-", Const(10), Const(2))
        # Interpreted evaluation
        assert exp4(None) == 8
        # Compiled evaluation
        expr = exp4.compile()
        assert eval(expr) == 8

if __name__ == '__main__':
  unittest.main()
