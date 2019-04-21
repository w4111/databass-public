from databass import *
import unittest
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
        preds = cond_to_func("(A.a = 2) and (A.b = B.m)")
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

        assert answer == [(20.0, 422.0)]


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

        preds = cond_to_func("(A.a = 2) and (B.b = C.f) and (E.a = F.b) and (E.b = G.c) and (F.c = G.d)")
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

        print answer

        assert answer == [(1312820.5128205128, 1537877.4358974358), (65641.02564102564, 93774.8717948718), (3282.051282051282, 21569.74358974359), (820.5128205128206, 4831.282051282052), (205.1282051282051, 646.6666666666667), (10.256410256410255, 421.02564102564105)]



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


        preds = cond_to_func("(A.a = 2) and (A.b = B.c) and (B.a = C.m) and (D.a = E.o) and (F.c = G.m) and (G.s = H.z)")
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

        assert answer == [(91428.57142857143, 975913.4285714285), (22857.14285714286, 509627.71428571426), (22857.14285714286, 50199.14285714285), (1142.857142857143, 25056.28571428571), (1142.8571428571427, 2084.8571428571427), (57.14285714285714, 827.7142857142857), (20.0, 422.0)]


if __name__ == '__main__':
  unittest.main()

