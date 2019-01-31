<img src="./docs/databass-small.png" width="600"></img>


# DataBass: Not Quite a DataBase

This is a simple Python-based analytical database for instructional purposes.  See the [system design](./docs/design.md) for details.  It is meant to give students exposure to query processing concepts in a system that goes from SQL strings to query results.

If you had fun doing the assignments for this, the course [W6113](https://w6113.github.io) features assignments based on a more advanced version of DataBass.

## Getting Started


Installation

    git clone git@github.com:w4111/databass-public.git

    # turn on virtualenv

    pip install click pandas numpy parsimonious readline nose


If you are a Columbia student and [have a clic account](https://www.cs.columbia.edu/~crf/accounts/cs.html), you can install and edit databass on clic.  That way you can minimize computer environment issues:

    # ssh into clic
    ssh <your user name>@clic.cs.columbia.edu

    # create virtual environment and enable it
    mkvirtualenv test
    workon test

    git clone git@github.com:w4111/databass-public.git
    pip install click pandas numpy parsimonious readline nose


### Take DataBass for a Spin.  

Do the following to run the DataBass console:

    python -m databass.prompt

Below is an example session using the prompt.  The user input is the text after the `> ` character.


	Welcome to DataBass.
	Type "help" for help, and "q" to exit
	> help

	List of commands

    [query]                           runs query string
    PARSE [query or expression str]   parse and print AST for expression or query
    TRACE                             print stack trace of last error
    SHOW TABLES                       print list of database tables
    SHOW <tablename>                  print schema for <tablename>


You can see how simple expressions are parsed:

	> parse 1+2*a
	1.0 + 2.0 * a

	> parse (1+2*a) / 10
	(1.0 + 2.0 * a) / 10.0

Or the parsed query plan of a SQL query

	> parse SELECT 1+2*a AS a FROM data WHERE a > 1
	Project(1.0 + 2.0 * a AS a)
	  WHERE(a > 1.0)
		FROM()
		  Scan(data AS data)

When the program starts, DataBass automatically crawls all subdirectories and loads any CSV files that it finds into memory.  In our example, [src/engine/data](./src/databass/data) contains two CSV files: [data.csv](../src/databass/data/data.csv) and [iowa-liquor-sample.csv](./src/databass/data/iowa-liquor-sample.csv).

	> show tables
	data
	iowa-liquor-sample

	> show data
	Schema for data
	a       <type 'int'>
	b       <type 'int'>
	c       <type 'int'>
	d       <type 'int'>

You can execute a simple query, and it will print the query plan and then the result rows.  Notice that SQL keywords need to be CAPITALIZED:

	> SELECT 1
	Project(1.0 AS attr0)
	{'attr0': 1.0}

	> select 1
	('ERROR:', Rule 'query' didn't match at 'select 1' (line 1, column 1).)

	> SELECT * FROM data LIMIT 2
	LIMIT(2.0)
	  Project(* AS None)
		Scan(data AS data)
	{'a': 1, 'c': 3, 'b': 2, 'd': 4}
	{'a': 1, 'c': 6, 'b': 5, 'd': 7}

### Running helper scripts

You can run some other helper scripts in the `databass/` folder:

    # python -m databass.<filename>
    python -m databass.parse --help

### Run Tests

To run tests, use the `nose` python test framework by specifying which tests in the `test/` directory to run:

    nosetests test/aa1.py

    # run all tests
    nosetests test/*.py

