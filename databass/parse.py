import click
from parse_expr import parse as parse_expr
from parse_sql import parse as parse_sql

if __name__ == "__main__":
  import click

  @click.command()
  @click.option("-e", type=str)
  @click.option("-q", type=str)
  def run(e=None, q=None):
    if e:
      print(e)
      ast = parse_expr(e)
      print(ast)
    if q:
      print(q)
      ast = parse_sql(q)
      print(ast)

  run()

