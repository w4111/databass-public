from exprs import *
from db import Database

    
class CodeBlock(object):
  """
  A helper class that the compiler uses to construct the compiled
  python program.  It mainly helps manage indentation
  """
  def __init__(self):
    self.lines = []
    self.b_indent_next = False

  def compile(self):
    """
    Turn the code block into a function that takes db as input and 
    executes the compiled code
    """
    code = []
    for (indent, l) in self.lines:
      code.append("%s%s" % ("  " * indent, l))
    return "\n".join(code)

  @property
  def cur_indent(self):
    indent = 0
    if self.lines:
      indent = self.lines[-1][0]
    if self.b_indent_next == True:
      indent += 1
    return indent

  def add_line(self, line):
    self.lines.append([self.cur_indent, line])
    self.b_indent_next = False

  def indent_all_lines(self, by=1):
    """
    @by amount to increase the indentation for all lines
    """
    for p in self.lines:
      p[0] += by

  def indent_next(self):
    self.b_indent_next = True

class Context(object):
  """
  The context object used throughout query compilation.  It primarily
  manages the sequence of operators that need to be evaluated 
  as well as the code that is being generated
  """
  def __init__(self):
    # stack of operators populated during produce phase
    self.stack = []  
    self.code = CodeBlock()

  def consume_next(self):
    if self.stack:
      self.stack.pop().consume(self)





class Op(object):
  def __iter__(self):
    raise Exception("Op: __iter__ not implemented")

  def produce(self, ctx):
    """
    @ctx context object

    The produce implementation for most operators in this assignment.  Some operators such as Join override this 
    to perform special code generation logic
    """
    if hasattr(self, "c"):
      # XXX: implement this 
      # the operator has a child, so need to maintain the stack and continue the producer phase
      pass
    else:
      # XXX: implement this 
      # the operator doesn't have a child, so done with the producer phase and should start consumer phase
      pass

  def consume(self, ctx):
    raise Exception("Op.consume not implemented")

  def compile_to_code(self):
    """
    @return a function that executes the compiled query.  The function takes as input a Database object

    Helper function that compiles the plan rooted at the current operator.
    It sets up the context and wraps the generated code into a function called f().
    """
    ctx = Context()
    # generate compiled code
    self.produce(ctx)

    # wrap compiled code into a function and turn code into a python function
    # that is returned to the caller
    ctx.code.indent_all_lines(1)
    ctx.code.lines.insert(0, [0, "def f(db):"])
    codeblock = ctx.code.compile()
    return codeblock

  def compile(self):
    codeblock = self.compile_to_code()
    exec(codeblock)
    return f

class Scan(Op):
  def __init__(self, tablename, db):
    """
    @tablename name of table to scan
    @db the Database object.  Used if Scan is interpreted rather than compiled
    """
    self.tablename = tablename
    self.db = db

  def __iter__(self):
    for row in self.db[self.tablename]:
      yield row

  def consume(self, ctx):
    """
    @ctx context object

    Adds code to ctx.code object for the Scan operator
    The compiled code assumes that "db" is within scope can can be referenced
    """
    # XXX: implement this method
    return

class Join(Op):
  def __init__(self, l, r, expr):
    self.l = l
    self.r = r
    self.expr = expr
    self.state = 0

  def __iter__(self):
    for lrow in self.l:
      for rrow in self.r:
        if self.cond(lrow, rrow):
          newtup = dict()
          newtup.update(lrow)
          newtup.update(rrow)
          yield newtup

  def produce(self, ctx):
    """
    @ctx context object

    Example implementation of the producer for a Join operator.  It will help to
    draw out the produce and consume phases to understand how the calls work to generate
    the appropriate join looping code.
    """
    ctx.stack.append(self)
    self.l.produce(ctx)

  def consume(self, ctx):
    """
    @ctx context object

    Example implementation of the consumer for the Join operator
    """
    if self.state == 0:
      self.state = 1
      ctx.stack.append(self)
      self.r.produce(ctx)
      ctx.code.add_line("if %s" % self.expr.compile())
      ctx.code.indent_next()


class Filter(Op):
  def __init__(self, c, exprs):
    self.c = c
    self.exprs = exprs
      
  def __iter__(self):
    for row in self.c:
      if all(e(row) for e in self.exprs):
        yield row

  def consume(self, ctx):
    """
    @ctx context object

    Addes code to ctx.code that applies the filter
    """
    # XXX: implement this method

                
class Project(Op):
  def __init__(self, c, exprs, aliases=[]):
    """
    @p            parent operator
    @exprs        list of function that take the tuple as input and
                  outputs a scalar value
    @aliases      name of the fields defined by the above exprs
    """
    self.c = c
    self.exprs = exprs
    self.aliases = list(aliases) or []
    self.set_default_aliases()

  def set_default_aliases(self):
    """
    This makes sure that each projection expression has an alias, because the alias is the key in the output row.
    """
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
    for row in self.c:
      ret = dict()
      for exp, alias in zip(self.exprs, self.aliases):
        ret[alias] = exp(row)
      yield ret

  def consume(self, ctx):
    """
    @ctx context object

    Adds code to the ctx.code object that sets the "row" variable to the result of applying the projection operation.
    """
    # XXX: Implement this method
    return


class Yield(Op):
  def __init__(self, c):
    self.c = c 

  def __iter__(self):
    return iter(self.c)

  def produce(self, ctx):
    """
    @ctx context object

    Generates code so that each result row is yielded to the code that calls the compiled query function
    """
    ctx.stack.append(self)
    self.c.produce(ctx)
    ctx.code.add_line("yield row")

  def consume(self, ctx):
    ctx.consume_next()

class Print(Op):
  def __init__(self, c):
    self.c = c

  def __iter__(self):
    for row in self.c:
      print row
    yield None

  def consume(self, ctx):
    """
    @ctx context object

    this method will generate printing code and add the code to the Code object in the context
    """
    # XXX: Implement this method
    return

class Count(Op):
  """
  This is a super hacky "aggregation" operator than only computes a count.
  It emits a single record with a single attribute "count".
  """
  def __init__(self, c):
    self.c = c

  def __iter__(self):
    n = 0
    for row in self.c:
      n += 1
    yield dict(count=n)

  def produce(self, ctx):
    """
    @ctx context

    Generates counter initialization and aggregation output code
    """
    # XXX: Implement this method so that it initializes a counter BEFORE the for loops
    #      put generates the row that represents the aggregation result AFTER the for loops
    
  def consume(self, ctx):
    """
    @ctx context object

    Generates code to update the counter
    """
    # XXX: Implement this method
    return


if __name__ == "__main__":
  import timeit
  import random
  random.seed(0)

  # Let's make fake data and add the table to the database
  data = [dict(a=random.randint(0, 100), b=random.randint(0, 100), c=random.randint(0, 100), i=i)
          for i in xrange(10000)]
  db = Database()
  db.register_table("data", data)

  # This creates a simple filtering and projection query equivalent to:
  #
  #   SELECT COUNT(*)
  #   FROM   (SELECT a AS a, b+c AS b 
  #             FROM data 
  #            WHERE a < 9+1+1)
  #   WHERE a < b
  # 
  add_expr = Expr("+", Expr("+", Const(9), Const(1)), Const(1))
  pred_expr = Expr("<", Var("a"), add_expr)
  filter_op = Filter(Scan("data", db), [pred_expr])
  proj_op = Project(filter_op, 
       [Var("a"), Expr("+", Var("b"), Var("c"))],
       ["a", "b"])
  filter_op = Filter(proj_op, [Expr("<", Var("a"), Var("b"))])
  q = Yield(Count(filter_op))


  # compile the query into a python function
  code = q.compile_to_code()
  print code

  assert list(q) == list(q.compile()(db))


  # Compare the query execution costs
  print "Interpreted\t", timeit.timeit(lambda: list(q), number=10)
  print "   Compiled\t", timeit.timeit(lambda: list(q.compile()(db)), number=10)

