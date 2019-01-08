import csv
import math
import inspect
import pandas
import numbers
import numpy as np
from collections import defaultdict

try:
  # hack to get this code to work on Instabase
  import instabase.notebook.ipython.utils as ib
  ib.import_pyfile('./parse_expr.py', 'parser')
except:
  pass

def cond_to_func(expr_or_func):
  """
  Helper function to help automatically interpret string expressions 
  when you manually construct a query plan.
  """
  from parse_expr import parse

  # if it's a function already, then we're good
  if hasattr(expr_or_func, "__call__"):
    return expr_or_func
  # otherwise, parse it as a string
  if isinstance(expr_or_func, str):
    return parse(expr_or_func)
  raise Exception("Can't interpret as expression: %s" % expr_or_func)


###################################################################
#
# These are the base operator classes
#
###################################################################

class Op(object):
  """
  Base class

  all operators have a single parent
  an operator may have multiple children
  """
  def __init__(self):
    self.p = None

  def __hash__(self):
    return hash(str(self))

  def __eq__(self, o):
    return o and hash(self) == hash(o)

  def replace(self, newop):
    """
    replace myself with @newop in the tree
    """
    if not self.p: return
    p = self.p
    if isinstance(p, UnaryOp):
      p.c = newop
    if isinstance(p, BinaryOp):
      if p.l == self:
        p.l = newop
      elif p.r == self:
        p.r = newop
    if isinstance(p, NaryOp):
      if self in p.cs:
        cs[cs.index(self)] = newop
      p.cs = cs

  def is_ancestor(self, anc):
    """
    Check if @anc is an ancestor of the current operator
    """
    n = self
    seen = set()
    while n and n not in seen:
      seen.add(n)
      if n == anc:
        return True
      n = n.p
    return False

  def children(self):
    """
    Go through all attributes of this object and return those that
    are subclasses of Op
    """
    children = []
    for key, attrval in self.__dict__.items():
      if key == "p": 
        continue
      if not isinstance(attrval, list):
        attrval = [attrval]
      for v in attrval:
        if v and isinstance(v, Op):
          children.append(v)
    return children

  def traverse(self, f, path=None):
    """
    Visit each operator in the query plan, and call f()
    @f a functions that takes as input the current operator and 
       the path to the operator
    """
    if path is None:
      path = []
    path = path + [self]
    f(self, path)
    for child in self.children():
      child.traverse(f, path)

  def is_type(self, klass_or_names):
    if not isinstance(klass_or_names, list):
      klass_or_names = [klass_or_names]
    names = [kn for kn in klass_or_names if isinstance(kn, str)]
    klasses = [kn for kn in klass_or_names if isinstance(kn, type)]
    return (self.__class__.__name__ in names or
           any([isinstance(self, kn) for kn in klasses]))

  def collect(self, klass_or_names):
    """
    Returns all operators in the subplan rooted at the current object
    that has the same class name, or is a subclass, as the arguments
    """
    ret = []
    if not isinstance(klass_or_names, list):
      klass_or_names = [klass_or_names]
    names = [kn for kn in klass_or_names if isinstance(kn, str)]
    klasses = [kn for kn in klass_or_names if isinstance(kn, type)]

    def f(node, path):
      if node and (
          node.__class__.__name__ in names or
          any([isinstance(node, kn) for kn in klasses])):
        ret.append(node)
    self.traverse(f)
    return ret

  def collectone(self, klassnames):
    """
    Helper function to return an arbitrary operator that matches any of the
    klass names or klass objects, or None
    """
    l = self.collect(klassnames)
    if l:
      return l[0]
    return None

  def to_str(self):
    """
    Return a description of the operator (ignoring children)
    """
    return ""

  def to_python(self):
    return self.to_str()

  def __str__(self):
    lines = []
    def f(op, path):
      if isinstance(op, ExprBase): return
      indent = "  " * (len(path) - 1)
      lines.append(indent + op.to_str())
    self.traverse(f)
    lines = filter(lambda l: l.strip(), lines)
    return "\n".join(lines)


class UnaryOp(Op):
  def __init__(self, c):
    super(UnaryOp, self).__init__()
    self.c = c
    if c:
      c.p = self

  def __setattr__(self, attr, v):
    super(UnaryOp, self).__setattr__(attr, v)
    if attr == "c" and v:
      self.c.p = self
 
class BinaryOp(Op):
  def __init__(self, l, r):
    super(BinaryOp, self).__init__()
    self.l = l
    self.r = r
    if l:
      l.p = self
    if r:
      r.p = self

  def __setattr__(self, attr, v):
    super(BinaryOp, self).__setattr__(attr, v)
    if attr in ("l", "r") and v:
      v.p = self
   
class NaryOp(Op):
  def __init__(self, cs):
    super(NaryOp, self).__init__()
    self.cs = cs
    for c in cs:
      if c:
        c.p = self

  def __setattr__(self, attr, v):
    super(NaryOp, self).__setattr__(attr, v)
    if attr == "cs":
      for c in self.cs:
        c.p = self
 

#######################################################
#
#  Definition of Query Plan operators
#
#######################################################


class Print(UnaryOp):
  def __iter__(self):
    for row in self.c:
      print row
    yield 

  def to_str(self):
    return "Print()"

class From(NaryOp):
  def to_str(self):
    #arg = ", ".join(["\t%s" % s for s in self.cs])
    return "FROM()"

class Source(UnaryOp):
  pass

class SubQuerySource(Source):
  def __init__(self, c, alias=None):
    super(SubQuerySource, self).__init__(c)
    self.alias = alias 

  def __iter__(self):
    for row in self.c:
      yield row

  def to_str(self):
    return "SubQuery(AS %s)" % (self.alias)

class Scan(Source):
  """
  A scan operator over a table in the Database.
  In order to run, it must have a reference to a Database object
  """

  def __init__(self, tablename, alias=None):
    self.tablename = tablename
    self.alias = alias or tablename
    self.db = None

  def set_db(self, db):
    self.db = db

  def __iter__(self):
    if self.db == None:
      raise Exception("Scan: Make sure to call Scan.set_db before executing iterator")

    for row in self.db[self.tablename]:
      yield row
  
  def to_str(self):
    return "Scan(%s AS %s)" % (self.tablename, self.alias)

class TableFunctionSource(UnaryOp):
  def __init__(self, function, alias=None):
    super(TableFunctionSource, self).__init__(function)
    self.function = function
    self.alias = alias 

  def __iter__(self):
    raise Exception("TableFunctionSource: Not implemented")

  def to_str(self):
    return "TableFunctionSource(%s)" % self.alias



class Join(BinaryOp):
  pass

class ThetaJoin(Join):
  """
  Theta Join is basically tuple-nested loops join
  """
  def __init__(self, l, r, cond="true"):
    """
    @l    left (outer) table of the join
    @r    right (inner) table of the join
    @cond a boolean function that takes as input two tuples, 
          one from the left table, one from the right
          OR
          an expression
    """
    super(ThetaJoin, self).__init__(l, r)
    self.cond = cond_to_func(cond) 

  def __iter__(self):
    for lrow in self.l:
      for rrow in self.r:
        if self.cond(lrow, rrow):
          newtup = dict()
          newtup.update(lrow)
          newtup.update(rrow)
          yield newtup

  def to_str(self):
    return "THETAJOIN(ON %s)" % (str(self.cond))

    
class HashJoin(Join):
  """
  Hash Join
  """
  def __init__(self, l, r, join_attrs):
    """
    @l    left table of the join
    @r    right table of the join
    @join_attrs two attributes to join on, hash join checks if the 
                attribute values from the left and right tables are
                the same.  Suppose:
                
                  l = iowa, r = iowa, join_attrs = ["STORE", "STORE"]

                then we return all pairs of (l, r) where 
                l.STORE = r.STORE
    """
    super(HashJoin, self).__init__(l, r)
    self.join_attrs = join_attrs

  def hash_func(self, val):
    """
    This function explicitly represents the hash key function for
    the hash table
    """
    return hash(val)

  def __iter__(self):
    """
    Build an index on the inner (right) source, then probe the index
    for each row in the outer (left) source.  
    
    Yields each join result
    """
    # Hash join is equality on left_attr and right_attr    
    left_attr = str(join_attrs[0])
    right_attr = str(join_attrs[1])

    # XXX: implement code to build the hash index

    # go through the outer source and probe the hashindex
    # make sure to use hash function to get the appropriate hash key
    # XXX: implement the join code here.  It should construct each 
    #      join result and "yield" it

  def build_hash_index(self, child_iter, attr):
    """
    @child_iter tuple iterator to construct an index over
    @attr attribute name to build index on

    Loops through a tuple iterator and creates an index based on
    the attr value
    """
    # defaultdict will initialize a hash entry to a new list if
    # the entry is not found
    index = defaultdict(list)
    for row in child_iter:
      # XXX: replace this code to populate the index
      pass
    return index
    
  def to_str(self):
    return "HASHJOIN(ON %s)" % (str(self.l), str(self.r), str(self.join_attrs))

      

class GroupBy(UnaryOp):
  def __init__(self, c, group_exprs):
    """
    @c           child operator
    @group_exprs list of functions that take the tuple as input and
                 outputs a scalar value
    """
    super(GroupBy, self).__init__(c)
    self.group_exprs = list(map(cond_to_func, group_exprs))

  def __iter__(self):
    hashtable = defaultdict(lambda: [None, None, []])
    for tup in self.c:
      key = tuple([e(tup) for e in op.group_exprs])
      hashtable[key][0] = key
      hashtable[key][1] = tup
      hashtable[key][2].append(tup)

    for _, (key, tup, group) in hashtable.items():
      tup = dict(tup)
      tup["__key__"] = key
      tup["__group__"] = group
      yield tup

  def to_str(self):
    return "GROUPBY(%s)" % (",".join(map(str, self.group_exprs)))


class OrderBy(UnaryOp):
  def __init__(self, c, order_exprs, ascdesc="asc"):
    """
    @c            child operator
    @order_exprs  ordered list of function that take the tuple as input 
                  and outputs a scalar value
    """
    super(OrderBy, self).__init__(c)
    self.order_exprs = list(map(cond_to_func, order_exprs))
    self.ascdesc = ascdesc
    self.normalize_ascdesc()

  def normalize_ascdesc(self):
    """
    make sure there is "asc" or "desc" for each element in order_exprs
    """
    if not self.ascdesc:
      self.ascdesc = []
    elif isinstance(self.ascdesc, str):
      self.ascdesc = [self.ascdesc]

    for i in range(len(self.order_exprs)):
      if len(self.ascdesc) < i:
        self.ascdesc.append("asc")

  def __iter__(self):
    raise Exception("ORDERBY.__iter__ is not implemented")

  def to_str(self):
    s = ", ".join(["%s %s" % (e, d) 
                for e, d in zip(self.order_exprs, self.ascdesc)])
    return "ORDERBY(%s)" % s


class Filter(UnaryOp):
  def __init__(self, c, cond):
    """
    @c            child operator
    @cond         boolean function that takes a tuple as input
    """
    super(Filter, self).__init__(c)
    self.cond = cond_to_func(cond)

  def __iter__(self):
    for row in self.c:
      if self.cond(row):
        yield row

  def to_str(self):
    return "WHERE(%s)" % str(self.cond)


# TODO: Edit this to support offset.  You will need to
#  1. change the constructor to take the offset as input
#  2. change the operator execution in __iter__ to support offset
#  3. change to_str() to also print the offset information
class Limit(UnaryOp):

  # TODO: Edit this constructor to take as input an offset expression
  def __init__(self, c, limit):
    """
    @c            child operator
    @limit        number of tuples to return
    """
    super(Limit, self).__init__(c)
    self.limit = limit
    if isinstance(self.limit, int):
      self.limit = Literal(self.limit)
  
    l =  int(self.limit(None))
    if l < 0:
      raise Exception("LIMIT must not be negative: %d" % l)

  def __iter__(self):
    # TODO: add code to enforce the offset.  Recall that the offset
    #       is allowed to be an expression!  
    #       You may assume that the expression will never reference an 
    #       attribute, and will always evaluate to a number
    _limit = int(self.limit(None))
    nyielded = 0
    for i, row in enumerate(self.c):
      if nyielded >= _limit:
        break
      nyielded += 1
      yield row

  def to_str(self):
    # TODO: This should also print the offset.
    #       There's no specific format that you need to adhere to,
    #       But we will check that it prints the offset expression
    return "LIMIT(%s)" % self.limit

class Distinct(UnaryOp):

  def __iter__(self):
    seen = set()
    for row in self.c:
      key = str(row)
      if key in seen: continue
      yield row
      seen.add(key)

  def to_str(self):
    return "DISTINCT()"

class Project(UnaryOp):
  def __init__(self, c, exprs, aliases=[]):
    """
    @p            parent operator
    @exprs        list of function that take the tuple as input and
                  outputs a scalar value
    @aliases      name of the fields defined by the above exprs
    """
    super(Project, self).__init__(c)
    self.exprs = list(map(cond_to_func, exprs))
    self.aliases = list(aliases) or []
    self.set_default_aliases()

  def set_default_aliases(self):
    for i, expr in enumerate(self.exprs):
      if i >= len(self.aliases):
        self.aliases.append(None)
      alias = self.aliases[i]
      if not alias:
        if isinstance(expr, Star): 
          continue
        if isinstance(expr, Attr):
          self.aliases[i] = expr.attr
        else:
          self.aliases[i] = "attr%s" % i

  def __iter__(self):
    child_iter = self.c

    # if the query doesn't have a FROM clause (SELECT 1),
    # then pass up an empty tuple
    if self.c == None:
      child_iter = [dict()]

    for row in child_iter:
      ret = dict()
      for exp, alias in zip(self.exprs, self.aliases):
        if isinstance(exp, Star):
          ret.update(exp(row))
        else:
          ret[alias] = exp(row)
      yield ret

  def to_str(self):
    args = ", ".join(["%s AS %s" % (e, a) for (e, a) in  zip(self.exprs, self.aliases)])
    return "Project(%s)" % args





###############################################################
#
#  The following are operators for simple Expressions 
#  used within Query Operators
#
#  e.g.,
#     f() 
#     1+2
#     T.a + 2 / T.b
#
###############################################################


def unary(op, v):
  """
  interpretor for executing unary operator expressions
  """
  if op == "+":
    return v
  if op == "-":
    return -v
  if op.lower() == "not":
    return not(v)

def binary(op, l, r):
  """
  interpretor for executing binary operator expressions
  """
  if op == "+": return l + r
  if op == "/": return l / r
  if op == "*": return l * r
  if op == "-": return l - r
  if op == "=": return l == r
  if op == "==": return l == r
  if op == "<>": return l != r
  if op == "!=": return l != r
  if op == "and": return l and r
  if op == "or": return l or r
  if op == "<": return l < r
  if op == ">": return l > r
  if op == "<=": return l <= r
  if op == ">=": return l >= r
  return True

class ExprBase(Op):
  def __str__(self):
    return self.to_str()

class Expr(ExprBase):
  def __init__(self, op, l, r=None):
    self.op = op
    self.l = l
    self.r = r

  def to_str(self):
    if self.r is not None:
      return "%s %s %s" % (self.l, self.op, self.r)
    return "%s %s" % (self.op, self.l)

  def to_python(self):
    op = self.op 
    if op == "=": op = "=="
    if self.r:
      return "%s %s %s" % (self.l.to_python(), op, self.r.to_python())
    return "%s %s" % (self.op, self.r.to_python())


  def __call__(self, tup, tup2=None):
    l = self.l(tup, tup2)
    if self.r is None:
      return unary(self.op, l)
    r = self.r(tup, tup2)
    return binary(self.op, l, r)

class Paren(UnaryOp, ExprBase):
  def to_str(self):
    return "(%s)" % self.c

  def __call__(self, tup, tup2=None):
    return self.c(tup)


class Between(ExprBase):
  def __init__(self, expr, lower, upper):
    """
    expr BETWEEN lower AND upper
    """
    self.expr = expr
    self.lower = lower
    self.upper = upper

  def to_str(self):
    return "(%s) BETWEEN (%s) AND (%s)" % (self.expr, self.lower, self.upper)

  def to_python(self):
    return "(%s) > (%s) && (%s) <= (%s)" % (
        self.expr.to_python(), self.lower.to_python(),
        self.expr.to_python(), self.upper.to_python())

  def __call__(self, tup, tup2=None):
    e = self.expr(tup, tup2)
    l = self.lower(tup, tup2)
    u = self.upper(tup, tup2)
    return e >= l and e <= u

class Func(ExprBase): 
  """
  This object needs to deal with scalar AND aggregation functions.
  """
  agg_func_lookup = dict(
    avg=np.mean,
    count=len,
    sum=np.sum,
    std=np.std,
    stddev=np.std
  )
  scalar_func_lookup = dict(
    lower=lambda s: str(s).lower()
  )


  def __init__(self, name, args):
    self.name = name.lower()
    self.args = args

  def to_str(self):
    args = ",".join(map(str, self.args))
    return "%s(%s)" % (self.name, args)

  def to_python(self):
    args = ",".join([a.to_python() for a in self.args])
    return "%s(%s)" % (self.name, args)


  def __call__(self, tup, tup2=None):
    f = Func.agg_func_lookup.get(self.name, None)
    if f:
      if "__group__" not in tup:
        raise Exception("aggregation function %s called but input is not a group!")
      args = []
      for gtup in tup["__group__"]:
        args.append([arg(gtup) for arg in self.args])

      # make the arguments columnar:
      # [ (a,a,a,a), (b,b,b,b) ]
      args = zip(*args)
      return f(*args)


    f = agg_func_lookup.get(self.name, None)
    if f:
      args = [arg(tup, tup2) for arg in self.args]
      return f(args)

    raise Exception("I don't recognize function %s" % self.name)

class Literal(ExprBase):
  def __init__(self, v):
    self.v = v

  def __call__(self, tup=None, tup2=None): 
    return self.v

  def to_str(self):
    if isinstance(self.v, str):
      return "'%s'" % self.v
    return str(self.v)


class Bool(ExprBase):
  def __init__(self, v):
    self.v = v
  def __call__(self, *args, **kwargs):
    return self.v
  def to_str(self):
    return str(self.v)

class Attr(ExprBase):
  def __init__(self, attr, tablename=None):
    self.attr = attr
    self.tablename = tablename

  def __call__(self, tup, tup2=None):
    if self.attr in tup:
      return tup[self.attr]
    if tup2 and self.attr in tup2:
      return tup2[self.attr]
    raise Exception("couldn't find %s in either tuple" % self.attr)

  def to_str(self):
    if self.tablename:
      return "%s.%s" % (self.tablename, self.attr)
    return self.attr

  def to_python(self):
    return """%s["%s"]""" % (self.tablename, self.attr)


class Star(ExprBase):
  def __init__(self, tablename=None):
    self.tablename = tablename
    if self.tablename:
      print("WARNING: can't deal with * for specific tables: %s" % self.tablename)

  def __call__(self, tup, tup2=None):
    return tup

  def to_str(self):
    if self.tablename:
      return "%s.*" % self.tablename
    return "*"

  def to_python(self):
    raise Exception("I don't support turning SELECT * into python code")


if __name__ == "__main__":
  import db
  db = db.Database()
  for row in HashJoin(db['data'], db['data'], ["a", "c"]):
    print row
