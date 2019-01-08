from exprs import *
from compiler import *
import unittest
from db import Database
#from parse_sql import parse
import random
import timeit

	#Each function below tests a different part of your code. 
	#As you develop your code, sequentially more of these tests should pass. 
	#When we grade, we will keep on the print statements for the code so we can check the logic
	#at each step. 

	#Commented under each unit test  are the functions in compiler.py that you will need to implement
	#to make the test pass. Good luck!
class TestUnits(unittest.TestCase):
    def test_all_functions(self):
        #All functions need to be implemented for this test to pass.
        data = [dict(a=random.randint(0, 100), b=random.randint(0, 100), c=random.randint(0, 100), i=i)
          		for i in range(10000)]

        db = Database()
       	db.register_table("data", data)
        # Full test:
       	#
       	#   SELECT COUNT(*)
        #   FROM   (SELECT a AS a, b+c AS b 
        #             FROM data 
        #            WHERE a < 9+1+1)
        #   WHERE a < b
        # 
        add_expr = Expr("+", Expr("+", Const(9), Const(1)), Const(1))
        pred_expr = Expr("<", Var("a"), add_expr)
        filter_op = Filter(Scan("data", db), [pred_expr]) #SELECT 
        proj_op = Project(filter_op, [Var("a"), Expr("+", Var("b"), Var("c"))], ["a", "b"])
        filter_op = Filter(proj_op, [Expr("<", Var("a"), Var("b"))])
        q = Yield(Count(filter_op))

        code = q.compile_to_code()
        print "\n\nOverall demo function is:"
        print code

        assert list(q) == list(q.compile()(db))

        #print "Query is: SELECT COUNT(*) FROM SELECT a AS a, b+c AS b FROM data WHERE a < 9+1+1 WHERE a<b"
        print "Overall test results:"
        print "Interpreted\t", timeit.timeit(lambda: list(q), number=10)
        print "Compiled\t", timeit.timeit(lambda: list(q.compile()(db)), number=10)

    def test_print(self):
        #Print/scan test
        #should work after updating the following:
        #Op.produce(ctx)
        #Print.consume(ctx) 
        #Scan.consume(ctx)
    
        data = [dict(a=random.randint(0, 100), b=random.randint(0, 100), c=random.randint(0, 100), i=i)
          		for i in range(10000)]

        db = Database()
       	db.register_table("data", data)
 
        scan_op = Scan("data", db) 
 
        code = scan_op.compile_to_code()
        print "\n\nPrint/scan test is"
        print code


        #check assertEquals

    def test_filter(self):
        #Filter test
        #should work after updating the following:
        #Filter.consume(ctx)

        data = [dict(a=random.randint(0, 100), b=random.randint(0, 100), c=random.randint(0, 100), i=i)
          		for i in range(10000)]

        db = Database()
       	db.register_table("data", data)

       	#   SELECT * 
        #       FROM data 
        #       WHERE a >= 3-2
         
        sub_expr = Expr("-", Const(2), Const(3))  #9+1-1
        pred_expr = Expr(">=", Var("a"), sub_expr) #WHERE a <= add_expr
        filter_op = Filter(Scan("data", db), [pred_expr]) #SELECT 
        q= Yield(filter_op)

        code = q.compile_to_code()
        print "\n\nFilter demo function is"
        print code

        assert list(q) == list(q.compile()(db))

        print "Filter test results are:"
        print "Interpreted\t", timeit.timeit(lambda: list(q), number=10)
        print "Compiled\t", timeit.timeit(lambda: list(q.compile()(db)), number=10)

    def test_project(self):
        #Project test
        #should work after updating the following:
        #Project.consume(ctx)


        data = [dict(a=random.randint(0, 100), b=random.randint(0, 100), c=random.randint(0, 100), i=i)
          for i in range(10000)]
      
        db = Database()
        db.register_table("data", data)
    
        data = [dict(a=random.randint(0, 100), b=random.randint(0, 100), c=random.randint(0, 100), i=i)
          		for i in range(10000)]

        db = Database()
       	db.register_table("data", data)
       	
        #   SELECT *
        #   FROM   (SELECT a AS a, b-c AS b 
        #             FROM data 
        #            WHERE a <= 5)
        #   WHERE a >= b
        # 
 
        add_expr = Expr("+", Const(5), Const(1))  
        pred_expr = Expr("<=", Var("a"), add_expr) 
        filter_op = Filter(Scan("data", db), [pred_expr])  
        proj_op = Project(filter_op, [Var("a"), Expr("-", Var("b"), Var("c"))], ["a", "b"]) #SELECT a AS a, b-c as b
        filter_op = Filter(proj_op, [Expr(">=", Var("a"), Var("b"))]) #WHERE a>=b
        q = Yield(filter_op)

        code = q.compile_to_code()
        print "\n\nProject demo function is"
        print code

        print list(q.compile()(db))[:3]
        print list(q)[:3]
        import pdb; pdb.set_trace()
        assert list(q) == list(q.compile()(db))

        print "Project test results are:"
        print "Interpreted\t", timeit.timeit(lambda: list(q), number=10)
        print "Compiled\t", timeit.timeit(lambda: list(q.compile()(db)), number=10)

    def test_count(self):
        #Count test
        #should work after updating the following:
        #Count.produce()
        #Count.consume()

        data = [dict(a=random.randint(0, 100), b=random.randint(0, 100), c=random.randint(0, 100), i=i)
          for i in range(10000)]

        db = Database()
        db.register_table("data", data)

        add_expr = Expr("+", Expr("-", Expr("+", Const(9), Const(1)), Const(1)), Const(4)) #WHERE a <= 9+1-1
        pred_expr = Expr("<=", Var("a"), add_expr) 
        filter_op = Filter(Scan("data", db), [pred_expr]) #SELECT 
        q = Yield(Count(filter_op))

        code = q.compile_to_code()
        print "\n\nCount demo function is"
        print code

        assert list(q) == list(q.compile()(db))

        print "Count test results are:"
        print "Interpreted\t", timeit.timeit(lambda: list(q), number=10)
        print "Compiled\t", timeit.timeit(lambda: list(q.compile()(db)), number=10)


if __name__ == '__main__':
  unittest.main()

