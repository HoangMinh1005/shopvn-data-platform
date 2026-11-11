"""Fail-fast Spark predicates shared by data layers."""


def fail_if(df, predicate, description: str):
    if df.filter(predicate).limit(1).count():
        raise ValueError(description)
