from databass import *
import unittest
import random
import timeit

#These functions test whether your code is computing cardinalities correctly. 
#We've hardcoded in values from our master databass. 

#We're also checking whether your implementation beats the exhaustive_bestplan, 
#which if you follow the suggested directions, should automatically happen.

class TestUnits(unittest.TestCase):

    def tearDown(x):
        print """"

########################################

PASSING THE TEST CASES DOES NOT MEAN YOUR OPTIMIZER IS WORKING since we are not asserting anything in them.

Make sure your query plan matches the one provided in the comments of each test case.

########################################
"""

   
    def test_card_cost(self):
        

        f = From([
        Scan("data", "A"),
        Scan("data2", "B"),
        ])
  
        preds = cond_to_func("(b = 2) and (b = n)")
        w = Filter(f, preds)

        db = Database()
        opt = Optimizer(db)

        print "Test 1 join plan"
        print opt(w)

        '''
        Below is what your code should output:

Test 1 join plan
WHERE((b = n))
  THETAJOIN(ON True)
    Scan(data AS A)
    Scan(data2 AS B)
        '''
        
    def test_selfjoin(self):
    #test that self joins work better than exhaustive best plan (w.r.t computation time)  


        f = From([
        Scan("data", "A"),
        Scan("data", "B"),
        Scan("data", "C"),
        Scan("data", "D"),
        Scan("data", "E"),
        Scan("data", "F"),
        Scan("data", "G")
        ])
  
        preds = cond_to_func("(a = 2) and (b = f) and (a = b) and (b = c) and (c = d)")
        w = Filter(f, preds)
        print w
        db = Database()
        opt = Optimizer(db)
        print "Test 2 join plan"
        print opt(w)


        '''
        Below is what your code should output:
(don't have to print the lines beginning 'tested'; those are just for your debugging)

Test 2 join plan
tested A True 9260.0 1.0
tested B True 9260.0 1.0
tested C True 9260.0 1.0
tested D True 9260.0 1.0
tested E True 9260.0 1.0
tested A True 185260.0 1.0
tested B True 185260.0 1.0
tested C True 185260.0 1.0
tested D True 185260.0 1.0
tested A True 3705260.0 1.0
tested B True 3705260.0 1.0
tested C True 3705260.0 1.0
tested A True 74105260.0 1.0
tested B True 74105260.0 1.0
tested A True 1482105260.0 1.0
WHERE((a = 2.0) and (b = f) and (a = b) and (b = c) and (c = d))
  THETAJOIN(ON True)
    Scan(data AS A)
    THETAJOIN(ON True)
      Scan(data AS B)
      THETAJOIN(ON True)
        Scan(data AS C)
        THETAJOIN(ON True)
          Scan(data AS D)
          THETAJOIN(ON True)
            Scan(data AS E)
            THETAJOIN(ON True)
              Scan(data AS F)
              Scan(data2 AS G)
 
        '''
    def test_multijoin(self):
    #test that multi joins work better than exhaustive best plan (w.r.t computation time)  
    

        f = From([
        Scan("data", "A"),
        Scan("data", "B"),
        Scan("data2", "C"),
        Scan("data", "D"),
        Scan("data2", "E"),
        Scan("data", "F"),
        Scan("data2", "G")
        ])
  
        preds = cond_to_func("(a = 2) and (b = c) and (a = b) and (b = c) and (c = d)")
        w = Filter(f, preds)
        print w
        db = Database()
        opt = Optimizer(db)
        print "Test 3 join plan"
        print opt(w)

'''
Below is what your code should print:
(don't have to print the lines beginning 'tested'; those are just for your debugging)

Test 3 join plan
tested A True 9260.0 1.0
tested B True 9260.0 1.0
tested C True 9260.0 1.0
tested D True 9260.0 1.0
tested E True 9260.0 1.0
tested A True 185260.0 1.0
tested B True 185260.0 1.0
tested C True 185260.0 1.0
tested D True 185260.0 1.0
tested A True 3705260.0 1.0
tested B True 3705260.0 1.0
tested C True 3705260.0 1.0
tested A True 74105260.0 1.0
tested B True 74105260.0 1.0
tested A True 1482105260.0 1.0
WHERE((a = 2.0) and (b = s) and (a = m) and (c = d))
  THETAJOIN(ON True)
    Scan(data AS A)
    THETAJOIN(ON True)
      Scan(data AS B)
      THETAJOIN(ON True)
        Scan(data2 AS C)
        THETAJOIN(ON True)
          Scan(data AS D)
          THETAJOIN(ON True)
            Scan(data2 AS E)
            THETAJOIN(ON True)
              Scan(data AS F)
              Scan(data2 AS G)
'''

if __name__ == '__main__':
  unittest.main()

