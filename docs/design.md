## System Design

This document describes the design of the DataBass query engine.  You will find the bulk of the code under [src/engine/](../src/engine/).

#### Background

Database is a reasonably featured, but simple, in-memory read-only database engine.  It can parse SQL queries, translate them into a query plan, perform simple optimizations such as join ordering, and run the query using either a push or pull-based execution model.  

The purpose is to provide an overview of how the main parts of a data query engine work together and introduce database concepts within the context of an end-to-end engine.  To do so, the engine does not support many things, such as insert/update/delete queries, transactions, recovery, memory-management, correct null value support, etc.

The engine is composed of the modules defined in Python files in the [src/engine/](../src/engine) folder:

* [db.py](../src/engine/db.py): this module manages the tables in the database.  It also keeps statistics about the tables that the optimizer can later use.
* [interpretor.py](../src/engine/interpretor.py): this module implements two types of execution methods.  The pull-based iterator method as described in class, and a push-based method that we briefly mentioned in class.  The assignments will focus on the pull-based iterator, however the push-based method is there for you to see how it could work.
* [ops.py](../src/engine/ops.py): this module implements the SQL operators and expression operators.  You will primarily edit this file to implement functionality.  Most of the code for the pull-based execution method is implemented as `__iter__` methods of the SQL operators defined in this file.
* [optimizer.py](../src/engine/optimizer.py): this module takes a query plan as input, and rewrites it to perform optimizations.  Your join order optimization assignment will primarily be implemented here.
* [parse_expr.py](../src/engine/parse_expr.py): this module is a simple parsing examples that only parses expressions and not queries.  You can play with it to get acquainted with how parsing works.
* [parse_sql.py](../src/engine/parse_sql.py): this module implements the subset of the SQL language that DataBass supports.  The parsing grammar rules also include those in `parse_expr`.
* [prompt.py](../src/engine/prompt.py): this is the DataBass client that you can use to write and execute SQL queries in the command line.


There is a self contained folder [src/complier](../src/compiler) that contains an extremely stripped down version of the query operators.  It implements the producer-consumer query compilation model and is [described in detail in the assignment instructions](https://w4111.github.io/advanced/compile).

## Core Components

This section describes how core database concepts such as Tables, Operators, the iterator execution model are designed.  The next section describes how the pieces connect together.

#### Tuples, Tables, Catalog

`db.py` primarily defines the `Table` and `Database` classes.  

DataBass represents tuples as Python dictionary objects.  In general, this is convenient, however there is a subtle issue that we will ignore.  Specifically when we concatenate two records (say for a Join), the following will occur:

        {a: 1, b: 2} CROSSPRODUCT {a: 3, b: 4} 

        # correct output if you run in Postgres/SQLite
        {a : 1, b: 2, a: 3, b:4}

        # output when using dictionary objects:
        {a: 3, b: 4}

Notice that the second record overwrote the first record.  We will live with this issue for our assignments.

`Table` provides an iterator interface over an in-memory table.  It keeps track of the field (attribute) names, and otherwise wraps around a list of python dictionaries, or a Pandas dataframe object.

`Database` manages the catalog of tables that can be queried.  It is basically a hash table that maps the table name to the Table object.  To make life easier, it automatically crawls the subdirectories ofthe directory that you run Python from, and load all CSV files that it finds into memory.

#### Operators

`ops.py` defines two types of operators: Query Operators and Expression Operators.  They all subclass `Op`.

`Op` is basically a tree node and provides a number of convenience functions for manipulating and traversing the query plan.  The main ones are:

* `collect(klasses)` traverses the tree and collects operators that are instances of the class names or objects in the argument
* `__str__()` performs a bunch of tree traversal magic to turn the query plan into a printable string.
* `to_str()` turns the current operator into a string (ignoring child operators).

The subclasses `UnaryOp`, `BinaryOp`, `NaryOp` are subclassed by the Query operators.  Under the covers, they manipulate the parent and child pointers to maintain the query plan.

##### Query Operators

Query Operators represent the logical and physical operators that we recognize, such as Filter (selection), Project, Join, LIMIT, etc.  You will notice that syntactic operators such as `From` is not actually executable.  The parser uses it to construct the parsed query plan, but the `From` operator needs to be replaced with a Join plan before the query can be run.  Similarly, there are also multiple implementations of the same logical operator.  For example, `ThetaJoin` and `HashJoin` are two implementations of Join.  

#### Expression Operators

Expression Operators are defined at the bottom of the file and represent expressions such as "a=b" or "f()".   The main difference from query operators is that query operators form the nodes of the query plan, while expressions are _used by a given operator_.  In addition, an expression operator can be directly evaluated by calling its `__call__(tup, tup2)` method.  The method takes up to two tuples as input because query operators are either unary (e.g., Project) or binary (e.g., Join).  

For example, the number `1` is represented as a `e = Literal(1)` expression operator.   Calling `e()` runs the `__call__()` method, evaluates the operator and returns the value `1`.  

Note that this is a big simplification for how things actually work because our expression operators ignore important information such as the table name of an attribute reference.  For example, `T.a` references attribute `a` in the table with alias `T`.  In reality, we evaluate `T.a` by looking an the two input tuples and picking the first instance of `a` (basically it doesn't make sure that `a` came from table `T`).  Similarly, expressions that combine many tables such as `A.a+B.b > C.c+D.d` are basically evaluated as `a+b > c+d`.


### Iterator Execution Model

Each query operator implements the `__iter__()` method, which returns an iterator over the subplan rooted at the operator.   Internally, the operator then iterates over its child operators to compute its results.  Take a look at how the `Scan` operator is implemented for a simple example.

In Python, using the `yield` keyword turns a function into an iterator.  This [stackoverflow answer is a good description](https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do).

`interpretor.py` implements two interpretors.  The pull-based interperetor is the one discussed in class.  It simply iterates over the root of the query plan, which will recursively iterate over the rest of the operators in the plan, to produce results.

The push-based interpretor is implemented simply to illustrate that there are other ways to execute a query plan.  The push-based method ultimately iterates over the input tables and calls a special method that represnts the query for each operator.  It is very similar to the query compiler you will implement. 

The root operator constructs a call-back method that it passes to its child to call on every tuple.  Recursively, the child constructs its own call-back and passes it to its own child.  For example, we have the query plan that prins each record emitted from the scan.

         Print
           |
        Project(1 AS a)
           |
          Scan

The Print operator constructs a callback that should be executed for each input tuple:

        def print_cb(tuple):
          print tuple

It then passes `print_cb` to the child Project operator, which constructs its own callback that calles `print_cb` for each projected tuple.

        def project_cb(tuple):
          newtuple = dict(a=1)
          print_cb(newtuple)

Scan operator  blindly executes its callbacks for each tuple that it scans:

        for tuple in Table:
          project_cb(tuple)

## Putting It Together

DataBass executes queries using the following workflow:

        query --> [ parser ] --> parsed query plan
              --> [ optimizer ] --> physical query plan 
              --> [ interpretor ] --> result tuples

The parsed query plan contains operators such as `From` that cannot be directly executed (doesn't implement `__iter__()`).  The optimizer replaces these with physical operators that are executable, and also computes a join plan.  The interpretor simply loops through the query plan's results.


