from ops import *
from itertools import *
from collections import *

def pickone(l, attr):
  return [(i and getattr(i, attr) or None) for i in l]

def flatten(list_of_lists):
  ret = []
  map(ret.extend, list_of_lists)
  return ret


class Optimizer(object):
  def __init__(self, db):
    self.db = db

  def __call__(self, op):
    if not op: return None

    # If there's a From operator in the tree, 
    # then replace with join tree
    while op.collectone("From"):
      op = self.expand_from_op(op)
    return op

  def expand_from_op(self, op):
    """
    Replace the first From operator under op with a join tree
    The algorithm is as follows

    0. Find first From operator F
    1. Find all binary expressions in any Where clause (Filter operator)
       that is an ancestor of F
    2. Keep the equality join predicates that only reference tables in
       the operator F
    3. Pick a join order 
    """

    # pick the first From clause to replace with join operators
    fromop = op.collectone("From")
    sources = fromop.cs
    sourcealiases = [s.alias for s in sources]

    # get all equi-join predicates 
    filters = op.collect("Filter")
    preds = []
    for f in filters:
      if fromop.is_ancestor(f):
        for e in f.collect(Expr):
          if self.valid_join_expr(e, sources):
            preds.append(e)

    join_tree = None
    for source in sources:
      if join_tree is None:
        join_tree = source
      else:
        join_tree = ThetaJoin(join_tree, source, Bool(True))
    
    
    # XXX: Uncomment the following two lines of code and run this file to
    #      try out our naive Selinger implementation.  Note that the current
    #      implementation does NOT generate good plans because you need to implement
    #      cost and cardinality estimates first!
    # opt = SelingerOpt(self.db)
    # join_tree = opt(preds, sources)

    fromop.replace(join_tree)
    return op

  def valid_join_expr(self, expr, sources):
    """
    @expr     candidate join expression
    @sources  the tables/sources of interest

    Checks that expression is a valid join expression.
    A valid join expression is a = operation that compares
    an attribute in each table

    e.g.,
         T.a = S.b            -- is valid
         T.a = T.b            -- not valid
         T.a = S.b + 1        -- not valid
    """
    if expr.op != "=":
      return False

    # get the alias names of all sources in the query
    aliases = pickone(sources, "alias")

    # helper functions
    unique_tablenames = lambda attrs: set(pickone(attrs, "tablename"))
    is_valid_ref = lambda ref: ref in aliases

    if not (isinstance(expr.l, Attr) and isinstance(expr.r, Attr)):
      return False

    lref = expr.l.tablename
    rref = expr.r.tablename

    if lref is None or rref is None:
      return False
    if not all(map(is_valid_ref, [lref, rref])):
      return False
    if lref == rref:
      return False
    return True



class SelingerOpt(object):
  def __init__(self, db):
    self.db = db
    self.costs = dict()
    self.cards = dict()

    self.DEFAULT_SELECTIVITY = 0.05

  def __call__(self, preds, sources):
    self.sources = sources
    self.preds = preds
    self.pred_index = self.build_predicate_index(preds)
    self.plans_tested = 0

    # This is an exhaustive algorithm that uses recursion
    # You will implement a faster bottom-up algorithm based on Selinger
    plan = self.best_plan_exhaustive(sources)
    
    # XXX: Uncomment the following once you have implemented best_plan()
    # plan = self.best_plan(sources)


    # print "# plans tested: ", self.plans_tested
    return plan


  def build_predicate_index(self, preds):
    """
    @preds list of join predicates to index

    Build index to map a pair of tablenames to their join predicates
    e.g., 
    
      SELECT * FROM A,B WHERE A.a = B.b 
   
    creates the lookup table:
   
      A,B --> "A.a = B.b"
      B,A --> "A.a = B.b"
   """
    pred_index = defaultdict(list)
    for pred in preds:
      lname = pred.l.tablename
      rname = pred.r.tablename
      pred_index[(lname,rname)] = pred
      pred_index[(rname,lname)] = pred
    return pred_index

  def get_join_pred(self, l, r):
    """
    @l left subplan
    @r right Scan operator

    This method looks for any predicate that involves a table in the left
    subplan and right Scan operator.  If it can't find a predicate, then it 
    returns the predicate True
    """
    if l.is_type(Scan):
      key = (l.alias, r.alias)
      return self.pred_index.get(key, Bool(True))
    for lsource in l.collect("Scan"):
      key = (lsource.alias, r.alias)
      if key in self.pred_index:
        return self.pred_index[key]
    return Bool(True)


  def best_plan(self, sources):
    """
    @sources list of tables that we will build a join plan for

    This implements a Selinger-based Bottom-up join optimization
    and returns a left-deep ThetaJoin plan.  The algorithm 

    1. picks the best 2-table join plan
    2. then iteratively picks the next table to join based on
       the cost model that you will implement.  

    """
    # make a copy of sources 
    sources = list(sources)

    # No need for optimizer if only one table in the FROM clause
    if len(sources) == 1:
      return sources[0]

    best_plan = self.best_initial_join(sources)
    sources.remove(best_plan.l)
    sources.remove(best_plan.r)

    # each iteration of this while loop adds the best table to join
    # with current best plan
    while sources:
      best_cand = None
      best_cost = float("inf")
      for r in sources:
        self.plans_tested += 1
        pred = self.get_join_pred(best_plan, r)

        
        # XXX: Write code to construct a candidate plan with r as the
        # inner table, and compute its cost using self.cost()
        #
        # Keep the lowest cost candidate plan in best_cand
        pass

      best_plan = best_cand
      sources.remove(best_plan.r)

    return best_plan


  def best_initial_join(self, sources):
    """
    @sources base taobles

    Try all 2-table join candidates and return the one with
    the lowest cost, as defined by self.cost()
    """
    best_plan = None
    best_cost = float("inf")

    for (l, r) in product(sources, sources):
      if l == r: continue
      self.plans_tested += 1
      pred = self.get_join_pred(l, r)

      # XXX: Write your code here

    return best_plan

  def best_plan_exhaustive(self, sources):
    """
    @sources list of tables that we will build a join plan for
    @return A left-deep ThetaJoin plan

    This is an example implementation of a exhaustive plan optimizer.
    It is slower than the bottom-up Selinnger approach
    that you will implement because it ends up checking the same candidate
    plans multiple times.  

    This code is provided to give you hints about how to use the class 
    methods and implement the bottom-up approach
    """
    if len(sources) == 1: return sources[0]

    best_plan = None
    best_cost = float("inf")
    for i, table in enumerate(sources):
      rest = sources[:i] + sources[i+1:]
      rest_plan = self.best_plan_exhaustive(rest)
      if rest_plan is None:
        continue

      pred = self.get_join_pred(rest_plan, table)
      plan = ThetaJoin(rest_plan, table, pred)
      cost = self.cost(plan)

      self.plans_tested += 1

      if cost <= best_cost:
        best_plan = plan
        best_cost = cost

    return best_plan



  def cost(self, join):
    """
    @join a left-deep join subplan
    @returns join cost estimate

    Estimate the cost to execute this join subplan
    """
    # internally cache cost estimates so they don't need to be recomputed
    if join in self.costs:
      return self.costs[join]

    # the input is actually a Scan operator
    if join.is_type(Scan):
      # XXX: Implement the cost to scan this Scan operator
      # Take a look at db.py:Stats, which provides some database statistics.
      # To use its functionality, you may need to implement parts of db.py
      cost = 0
    else:
      # XXX: Compute the cost of the tuple-based nested loops join operation
      # in terms of the cost to compute the outer (left) subplan and the number
      # of tuples we need to examine from the inner (right) table.
      #
      # Hint: You may want to compute the cost recursively.
      cost = 0

      # We penalize high cardinality joins a little bit
      cost += 0.1 * self.card(join)

    # save estimate in the cache
    self.costs[join] = cost
    return cost

  def card(self, join):
    """
    @join join subplan 
    @returns join cardinality estimate

    Compute the cardinality estimate of the join subplan
    """
    # We cache the cardinality estimates
    if join in self.cards:
      return self.cards[join]

    if join.is_type(Scan):
      # XXX: Compute the cardinality of the join if it is a Scan operator
      # Similar to self.cost() above, take a look at db.py:Stats.
      card = 1
    else:
      # XXX: Compute the cardinality of the join subplan as described in lecture.
      # Hint: You may want to compute the cardinality recursively
      card = 1

    # Save estimate in the cache
    self.cards[join] = card
    return card

  def selectivity(self, join):
    """
    @join join subplan

    Computes the selectivity of the join depending on the number of
    tables, the predicate, and the selectivities of the join attributes
    """
    
    if join.is_type(Scan):
      return self.DEFAULT_SELECTIVITY

    # if the predicate is a boolean, then the selectivity
    # is 1 if True (cross-product), or 0 if False
    if join.cond.is_type(Bool):
      return join.cond() * 1.0

    lsel = self.selectivity_attr(join.l, join.cond.l.attr)
    rsel = self.selectivity_attr(join.r, join.cond.r.attr)
    return min(lsel, rsel)

  def selectivity_attr(self, source, attr):
    """
    @source the left or right subplan
    @attr  the attribute in the subplan used in the equijoin

    Estimate the selectivity of a join attribute.  
    We make the following assumptions:

    * if the source is not a base table, then the selectivity is 1
    * if the attribute is numeric then we assume the attribute values are
      uniformly distributed between the min and max values.
    * if the attribute is non-numeric, we assume the values are 
      uniformly distributed across the distinct attribute values
    """
    if not source.is_type(Scan):
      return 1.0

    table = self.db[source.tablename]
    stat = table.stats[attr]
    if table.type(attr) == "num":
      # XXX: Write code to estimate the selectivity of the numeric attribute.
      # You can add 1 to the denominator to avoid divide by 0 errors
      sel = 1.0
    else:
      # XXX: Write code to estimaote the selectivity of the non-numeric attribute
      sel = 1.0
    return sel



if __name__ == "__main__":
  from db import Database

  f = From([
      Scan("data", "A"),
      Scan("data", "B"),
      Scan("data", "C"),
      Scan("data", "D"),
      Scan("data", "E"),
      Scan("data", "F"),
      Scan("data", "G")
    ])
  preds = cond_to_func("(A.a = 1) and (A.a = B.a) and (B.b = C.b) and (C.c = D.c) and (D.a = E.b) and (E.b = F.c) and (F.c = G.d)")
  w = Filter(f, preds)
  print w
  db = Database()
  opt = Optimizer(db)
  print opt(w)
