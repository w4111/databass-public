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
        pass

    def test_easyjoin(self):
        f = From([
        Scan("data", "A"),
        Scan("data2", "B"),
        ])
        print "test join"
        preds = cond_to_func("(a = 2) and (b = m)")
        w = Filter(f, preds)
        print w
        db = Database()
        opt = Optimizer(db)
        print "Test selingerOpt join plan"
        print opt(w)
        sel_opt = SelingerOpt(db)
        o = opt(w)
        joins = []

        def get_join(op):
            if isinstance(op, Join):
                joins.append(op)
            for c in op.children():
                get_join(c)

        get_join(o)
        answer = []
        for j in joins:
            answer.append((sel_opt.card(j), sel_opt.cost(j)))
        print answer
       
        assert answer == [(400.0, 460.0)]


    def test_selfjoin(self):
    # this part test the cost and cadinality of the each join node
    # You should observe that self joins with selingerOpt work better than exhaustive best plan (w.r.t computation time)
        print
        print "test2: selfjoin"
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
        print "Test selingerOpt join plan"
        print(opt(w))
        sel_opt = SelingerOpt(db)
        o = opt(w)
        joins = []

        def get_join(op):
            if isinstance(op, Join):
                joins.append(op)
            for c in op.children():
                get_join(c)

        get_join(o)
        answer = []
        for j in joins:
            answer.append((sel_opt.card(j), sel_opt.cost(j)))

        assert answer == [(1280000000.0, 1482105260.0), (64000000.0, 74105260.0), (3200000.0, 3705260.0), (160000.0, 185260.0), (8000.0, 9260.0), (400.0, 460.0)]


    def test_multijoin(self):
    # this part test the cost and cadinality of the each multi-join node
    # You should observe that self joins with selingerOpt work better than exhaustive best plan (w.r.t computation time)
        print
        print "test3: multijoin"
        f = From([
        Scan("data", "A"),
        Scan("data", "B"),
        Scan("data2", "C"),
        Scan("data", "D"),
        Scan("data2", "E"),
        Scan("data", "F"),
        Scan("data2", "G"),
        Scan("data3", "H")
        ])


        preds = cond_to_func("(a = 2) and (b = c) and (a = b) and (b = c) and (c = d) and (s = z)")
        w = Filter(f, preds)
        print w
        db = Database()
        opt = Optimizer(db)
        print "Test selingerOpt join plan"
        print opt(w)
        sel_opt = SelingerOpt(db)
        o = opt(w)
        joins = []

        def get_join(op):
            if isinstance(op, Join):
                joins.append(op)
            for c in op.children():
                get_join(c)

        get_join(o)
        answer = []
        for j in joins:
            answer.append((sel_opt.card(j), sel_opt.cost(j)))

        assert answer == [(25600000000.0, 29642105260.0), (1280000000.0, 1482105260.0), (64000000.0, 74105260.0), (3200000.0, 3705260.0), (160000.0, 185260.0), (8000.0, 9260.0), (400.0, 460.0)]

if __name__ == '__main__':
  unittest.main()

