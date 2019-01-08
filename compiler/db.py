import pandas
import os
try:
  import instabase.notebook.ipython.utils as ib
except:
  pass


openfile = open
try:
  openfile = ib.open
except:
  pass


class Table:
  def __init__(self, fields, rows):
    self.fields = fields
    self.rows = rows

  @property
  def stats(self):
    return Stat(self)

  @staticmethod
  def from_dataframe(df):
    fields = list(df.columns)
    rows = df.T.to_dict().values()
    return Table(fields, rows)

  @staticmethod
  def from_rows(rows):
    if not rows:
      return Table([], rows)
    return Table(rows[0].keys(), rows)

  def __iter__(self):
    return iter(self.rows)

class Database(object):
  """
  Looks for and registers all csv files in the current directory
  """
  def __init__(self):
    self.registry = {}
    self.setup()

  def setup(self):
    for root, dirs, files in os.walk("."):
      for fname in files:
        if fname.lower().endswith(".csv"):
          tablename, _ = os.path.splitext(fname)
          fpath = os.path.join(root, fname)
          try:
            with openfile(fpath) as f:
              df = pandas.read_csv(f)
              self.register_dataframe(tablename, df)
          except Exception as e:
            print("Failed to read data file %s" % fpath)
            print(e)

  def register_table(self, tablename, table):
    self.registry[tablename] = table

  def register_dataframe(self, tablename, df):
    self.register_table(tablename, Table.from_dataframe(df))

  @property
  def tablenames(self):
    return self.registry.keys()

  def __contains__(self, tablename):
    return tablename in self.registry

  def __getitem__(self, tablename):
    return self.registry.get(tablename, None)

  def statistics(self, tablename):
    pass
