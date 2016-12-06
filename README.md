# database_engine
  This is a preliminary database engine written in Java. It parses SQL statements using JSQLParser.
  Supported SQL queries include:
    Non-Aggregate Queries
        SelectItems may include:
            SelectExpressionItem: Any expression that ExpressionLib can evaluate.  Note that Column expressions may or may not include an appropriate source.  Where relevant, column aliases will be given, unless the SelectExpressionItem's expression is a Column (in which case the Column's name attribute should be used as an alias)
            AllTableColumns: For any aliased term in the from clause
            AllColumns: If present, this will be the only SelectItem in a given PlainSelect.
        
    Aggregate Queries
        SelectItems may include:
            SelectExpressionItems where the Expression is one of:
                A Function with the (case-insensitive) name: SUM, COUNT, AVG, MIN or MAX.  The Function's argument(s) may be any expression(s) that can be evaluated by ExpressionLib.
                A Single Column that also occurs in the GroupBy list.
            AllTableColumns: If all of the table's columns also occur in the GroupBy list
            AllColumns: If all of the source's columns also occur in the GroupBy list.
        GroupBy column references are all Columns.
        
    Both Types of Queries
        From/Joins may include:
            Join: All joins will be simple joins
            Table: Tables may or may not be aliased.  Non-Aliased tables should be treated as being aliased to the table's name.
            SubSelect: SubSelects may be aggregate or non-aggregate queries, as here.
        The Where/Having clauses may include:
            Any expression that ExpressionLib will evaluate to an instance of BooleanValue
        Allowable Select Options include
            SELECT DISTINCT (but not SELECT DISTINCT ON)
            UNION ALL (but not UNION)
            Order By: The OrderByItem expressions may include any expression that can be evaluated by ExpressionLib.  Columns in the OrderByItem expressions will refer only to aliases defined in the SelectItems (i.e., the output schema of the query's projection.  See TPC-H Benchmark Query 5 for an example of this)
            Limit: RowCount limits (e.g., LIMIT 5), but not Offset limits (e.g., LIMIT 5 OFFSET 10) or JDBC parameter limits.

  It implements selection, projection, join, union and other (extended) relational algebra operators.
  This is a graduate course project at SUNY,Buffalo. 
