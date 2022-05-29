/**
* Parses a explain module statement.
*/
SqlNode SqlRichExplain() :
{
    SqlNode stmt;
    Set<String> explainDetails = new HashSet<String>();
}
{
    <EXPLAIN>
    [
        <PLAN> <FOR>
        |
            ParseExplainDetail(explainDetails)
        (
            <COMMA>
            ParseExplainDetail(explainDetails)
        )*
    ]
    (
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        |
        stmt = RichSqlInsert()
    )
    {
        return new SqlRichExplain(getPos(), stmt, explainDetails);
    }
}