import timeit
from ops import *
from db import *
from interpretor import *
from optimizer import Optimizer
    
# Check that the join actually is faster by running on a larger dataset

#SELECT a*2, c-a FROM data, (SELECT a AS x, b AS y, c AS z FROM data) WHERE a = x AND a <= x LIMIT 5
ast1 = Limit(
        Project(
          Filter(
            ThetaJoin(
              Scan("data"), 
              Project(Scan("data"), ["a", "b", "c"], ["x", "y", "z"]),
              "a = x"),
            "a <= x"),
          ["a*2", "c-a"]
        ),
        5
      )

s = Scan("data")
g = GroupBy(s, ["a"])
p = Project(g, ["avg(b)", "avg(a)", "a"], ["avg", "avg2", "a"])
ast2 = p 

q1 = """SELECT avg(t.b) AS avg
       FROM data AS t, data AS b, data AS c, 
            (SELECT h.b, g.a * h.c AS z 
             FROM data as g, data as h 
             WHERE g.a < h.a
             LIMIT 1 ) AS f
       WHERE t.a = b.a AND 
             b.b = f.b
       GROUP BY f.z 
       LIMIT 1"""


q2 = """SELECT s.x, avg(t.b) AS avg, count(1) AS count
       FROM data AS t, 
            (SELECT a AS x, b AS y, c AS z FROM data) AS s
       WHERE t.c = s.z
       GROUP BY s.x"""

q3 = """SELECT x, avg(b) AS avg, count(1) AS count
       FROM data AS t, 
            (SELECT a AS x, b AS y, c AS z FROM data) AS s
       WHERE c = c
       GROUP BY x"""


# hash join
ast3 = OrderBy(
  Distinct(
    Project(
      Filter(
        HashJoin(
          Scan("./iowa-liquor-sample.csv"), 
          Project(
            Filter(
              Scan("./iowa-liquor-sample.csv"), 
              "CATEGORY = 1022100 and BOTTLE_QTY > 0"
            ), 
            ["STORE", "ITEM"], 
            ["s", "i"]
          ),
          ["STORE", "s"]
        ), "ITEM <> i"
      ), ["ZIPCODE"], ["ZIPCODE"]
    )
  ), ["ZIPCODE"]
)

# hashjoin
ast4 = OrderBy(
  Distinct(
    Project(
      ThetaJoin(
        Scan("./iowa-liquor-sample.csv"), 
        Project(
          Filter(
            Scan("./iowa-liquor-sample.csv"), 
            "CATEGORY = 1022100 and BOTTLE_QTY > 0"
          ), 
          ["STORE", "ITEM"], 
          ["s", "i"]
        ),
        "STORE = s and ITEM <> i"
      ), ["ZIPCODE"], ["ZIPCODE"]
    )
  ), ["ZIPCODE"]
)

asts = [ast1, ast2]#, ast3, ast4]
qs = [q1, q2, q3]
db = Database()
db.register_table("data", Table.from_rows(db["data"].rows * 5))
optimizer = Optimizer(db)
interpretor = PushBasedInterpretor(db)

def return_time(o):
  start_time = timeit.default_timer()
  interpretor(Print(o))
  elapsed = timeit.default_timer() - start_time
  return elapsed
 



for ast in asts:
  print()
  print(ast)
  print(return_time(ast))

for q in qs:
  print()
  print(q)
  ast = parse(q)
  ast = optimizer(ast)
  print(ast)
  print(return_time(ast))