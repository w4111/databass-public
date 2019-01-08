import traceback
import readline
import click

WELCOMETEXT = """Welcome to DataBass.  
Type "help" for help, and "q" to exit"""


HELPTEXT = """
List of commands

[query]                           runs query string
PARSE [query or expression str]   parse and print AST for expression or query
TRACE                             print stack trace of last error
SHOW TABLES                       print list of database tables
SHOW <tablename>                  print schema for <tablename>
"""



if __name__ == "__main__":

  @click.command()
  def main():
    print(WELCOMETEXT)
    service_inputs()

  def service_inputs():
    cmd = raw_input("> ").strip()

    import interpretor
    import optimizer
    import ops
    import db
    import parse_expr
    import parse_sql
    from interpretor import PullBasedInterpretor, PushBasedInterpretor
    from optimizer import Optimizer
    from ops import Print
    from db import Database
    from parse_expr import parse as _parse_expr
    from parse_sql import parse as _parse_sql


    _db = Database()

    if cmd == "q":
      return

    elif cmd == "":
      pass

    elif cmd.startswith("help"):
      print(HELPTEXT)

    elif cmd.lower() == "reload":
      reload(parse_expr)
      reload(db)
      reload(ops)
      reload(parse_sql)
      reload(optimizer)
      reload(interpretor)
      from parse_expr import parse as _parse_expr
      from db import Database
      from ops import Print
      from parse_sql import parse as _parse_sql
      from optimizer import Optimizer
      from interpretor import PullBasedInterpretor, PushBasedInterpretor


    elif cmd.upper().startswith("TRACE"):
      traceback.print_exc()

    elif cmd.upper().startswith("PARSE"):
      q = cmd[len("PARSE"):]
      ast = None
      try:
        ast = _parse_expr(q)
      except Exception as err_expr:
        try:
          ast = _parse_sql(q)
        except Exception as err:
          print("ERROR:", err)

      if ast:
        print(ast)


    elif cmd.upper().startswith("SHOW TABLES"):
      for tablename in _db.tablenames:
        print tablename
      
    elif cmd.upper().startswith("SHOW "):
      tname = cmd[len("SHOW "):].strip()
      if tname in _db:
        print "Schema for %s" % tname
        t = _db[tname]
        for field in t.fields:
          if t.rows:
            typ = type(t.rows[0][field])
          else:
            typ = "?"
          print field, "\t", typ
      else:
          print "%s not in database" % tname

    else:
      try:
        plan = _parse_sql(cmd)
        opt = Optimizer(_db)
        interp = PullBasedInterpretor(_db)
        #interp = PushBasedInterpretor(_db)

        plan = opt(plan)
        print(plan)
        for row in interp(plan):
          print row
      except Exception as err:
        print("ERROR:", err)

    del _db
    service_inputs()


  main()
