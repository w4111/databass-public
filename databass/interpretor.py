from collections import *
try:
  import instabase.notebook.ipython.utils as ib
  ib.import_pyfile('./ops.py', 'ops')
  ib.import_pyfile('./parser.py', 'parser')
except:
  pass
from ops import *
from parse_sql import parse



class PullBasedInterpretor(object):
  def __init__(self, db):
    self.db = db

  def __call__(self, op, **kwargs):
    # make sure the scan operators have access to the database
    for scanop in op.collect("Scan"):
      scanop.set_db(self.db)

    for row in op:
      yield row





class PushBasedInterpretor(object):
  def __init__(self, db):
    self.db = db
    self.limit_counters = []
    self.overlap_checkers = []

  def run_print(self, op, f):
    def print_f(tup):
      print(tup)
    self(op.c, print_f)

  def run_subquerysource(self, op, f):
    self(op.c, f)

  def run_scan(self, op, f):
    if op.tablename not in self.db:
      raise Exception("Table \"%s\" not found in database" % op.tablename)
    table = self.db[op.tablename]
    for tup in table:
      r = f(tup)
      if r == False:
        break

  def run_thetajoin(self, op, f):
    idx = len(self.overlap_checkers)
    self.overlap_checkers.append(False)
    def outer_loop(left):
      def inner_loop(right):
        if False and not self.overlap_checkers[idx]:
          self.overlap_checkers[idx] = True
          lkeys = left.keys()
          rkeys = right.keys()
          overlap = set(lkeys).intersection(rkeys)
          if overlap:
             print("WARNING: join has overlapping attributes: (%s)" % overlap)

        if op.cond(left, right):
          newtup = dict()
          newtup.update(left)
          newtup.update(right)
          return f(newtup)
      self(op.r, inner_loop)
    self(op.l, outer_loop)
    self.overlap_checkers.pop()

  def run_hashjoin(self, op, f):
    # Comment the following line and write your code here
    # raise Exception('Implement hash join!')

    # Hash join is equality on left_attr and right_attr    
    join_attrs = [str(x) for x in op.join_attrs]
    left_attr = join_attrs[0]
    right_attr = join_attrs[1]
    
    # Step 1. build a hash index (dictionary that maps join attribute value to list of tuples)
    # 1.1 TODO: define a dictionary of index in the next line
    m = defaultdict(list)
    
    # hash phase, remove pass and implement function add_to_index
    def add_to_index(tup):
        # 1.2 TODO: add tuple (from right table) to your index, key should be the value of right_attr of tup
        
        # Hint: use tup[right_attr] as a key in index dictionary, and the corresponding value
        #       is a list of tuples from the right side table with the same right_attr value
        # e.g., left_attr is 'STORE' and right_attr is 's', tup['s'] gives the store number 2515
        #       let's call index dictionary as m, you should add all tuples with s = 2515 to m[2515]
        m[tup[right_attr]].append(tup)
    
    # we now run the right side of the join, and for each tuple, run add_to_index
    self(op.r, add_to_index)
    
    # Step 2. for each left tuple, look up in your index:
    def lookup_left_tuple(tup):
        # 2.1 TODO: check if there is a match of the left tup attribute value in the index dictionary
        #     if so, perform the join (as in Nested Loops Join above), merge left and right tuple
        #     and call f() on the new tuple
        if m[tup[left_attr]]:
            for t in m[tup[left_attr]]:
                newtup = dict()
                newtup.update(tup)
                newtup.update(t)
                if f(newtup) == False:
                  return False
    
    # we now run the left side of the join, and for each tuple, run lookup_left_tuple
    self(op.l, lookup_left_tuple)

  def run_limit(self, op, f):
    idx = len(self.limit_counters)
    self.limit_counters.append(0)
    def limit_f(tup):
      if self.limit_counters[idx] >= op.limit():
        return False
      self.limit_counters[idx] += 1
      return f(tup)
    self(op.c, limit_f)
    self.limit_counters.pop()

  def run_groupby(self, op, f):
    hashtable = defaultdict(lambda: [None, None, []])
    def group_f(tup):
      key = tuple([e(tup) for e in op.group_exprs])
      hashtable[key][0] = key
      hashtable[key][1] = tup
      hashtable[key][2].append(tup)
    self(op.c, group_f)

    for _, (key, tup, group) in hashtable.items():
      tup = dict(tup)
      tup["__key__"] = key
      tup["__group__"] = group
      f(tup)

  def run_orderby(self, op, f):
    tup_buffer = []
    def order_f(tup):
      tup_buffer.append(tup)
    self(op.c, order_f)

  def run_project(self, op, f):
    def project_f(tup):
      ret = dict()
      for exp, alias in zip(op.exprs, op.aliases):
        if isinstance(exp, Star):
          ret.update(exp(tup))
        else:
          ret[alias] = exp(tup)
      return f(ret)
    self(op.c, project_f)

  def run_filter(self, op, f):
    def where_f(tup):
      if op.cond(tup):
        return f(tup)
    self(op.c, where_f)

  def run_distinct(self, op, f):
    tup_buffer = []
    def distinct_f(tup):
      if tup not in tup_buffer:
        tup_buffer.append(tup)
        return f(tup)
    self(op.c, distinct_f)


  def __call__(self, op, f=lambda t:t):
    """
    This function dispatches the current operator to the appropriate handler.  
    Each handler crafts the appropriate callbak function and sends it to the child op

    @op current operator to execute
    @f the function to call for every output tuple of this operator (op)
       if f() returns False, it is a signal that the current operator
       doesn't need to generate more tuples and can stop

    See code for Scan to see how it calls f() for each output tuple
    """
    klass = op.__class__.__name__

    if klass == "Print":
      self.run_print(op, f)
    elif klass == "Scan":
      self.run_scan(op, f)
    elif klass == "ThetaJoin":
      self.run_thetajoin(op, f)
    elif klass == "HashJoin":
      self.run_hashjoin(op, f)
    elif klass == "Limit":
      self.run_limit(op, f)
    elif klass == "GroupBy":
      self.run_groupby(op, f)
    elif klass == "OrderBy":
      self.run_orderby(op, f)
    elif klass == "Filter":
      self.run_filter(op, f)
    elif klass == "Project":
      self.run_project(op, f)
    elif klass == "Distinct":
      self.run_distinct(op, f)
    elif klass == "SubQuerySource":
      self.run_subquerysource(op, f)
    else:
      raise Exception("Did not recognize operator: %s" % klass)
    

