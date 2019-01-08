"""

The following are classes that implement simple expressions
such as +, >, /, and attribute lookups.  They are fully
implemented so you can read them for reference.

An expression is either a unary or binary operator.  

The __call__() method evaluates the expression on one or two records depending
on the arity of the expression.

The compile() method turns the expression into a single python string
that can be evaluated.  The compiled string assumes that there is a 
single row variable within the scope called "row"

"""


class Expr(object):
  """
  """
  def __init__(self, op, l, r):
    self.op = op
    self.l = l
    self.r = r

  def __call__(self, row, row2={}):
    if self.op == "<>":
      return self.l(row) != self.r(row)
    if self.op == "=":
      return self.l(row) == self.r(row)
    if self.op == "<":
      return self.l(row) < self.r(row)
    if self.op == ">":
      return self.l(row) > self.r(row)
    if self.op == "+":
      return self.l(row) + self.r(row)

  def compile(self):
    """
    XXX: Edit this code to add support for more types of binary operations
    """
    op = None
    if self.op == "=":
      op = "=="
    elif self.op == "<":
      op = "<"
    else:
      op = self.op
    return "(%s) %s (%s)" % (self.l.compile(), op, self.r.compile())

class Const(Expr):
  def __init__(self, v):
    self.v = v
      
  def __call__(self, row, row2={}):
    return self.v
  
  def compile(self):
    if isinstance(self.v, basestring):
      return "'%s'" % self.v
    return str(self.v)

class Var(Expr):
  def __init__(self, attr):
    self.attr = attr
      
  def __call__(self, row, row2={}):
    if self.attr in row:
      return row[self.attr]
    if self.attr in row2:
      return row2[self.attr]
    raise Exception("Var: couldn't find %s in input records" % self.attr)
  
  def compile(self):
    return "row['%s']" % self.attr



