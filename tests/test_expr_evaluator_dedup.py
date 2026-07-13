from neosqlite.collection.expr_evaluator import ExprEvaluator


def test_build_select_expression_delegation():
    evaluator = ExprEvaluator()
    sql, params = evaluator.build_select_expression(
        {"$sin": "$angle"}, alias="sin_val"
    )
    assert sql == "sin(json_extract(data, '$.angle')) AS sin_val"
    assert params == []
