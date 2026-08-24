"""Minimal ICCA SQL Server bridge for redsan.

This module intentionally contains transport only. SQL construction, identifier
mapping, and clinical semantics remain in R.
"""


def execute_sql(sql, params=None, keystore_path=None):
    """Execute a parameterized query against the d2im ICCAJ database."""
    try:
        from d2im.dbc import edsan_dbc
        from d2im.cfg import constants
    except ImportError as exc:
        raise RuntimeError(
            "The CHU Python package `d2im` is not importable in this Python "
            "environment."
        ) from exc

    kwargs = {
        "params": tuple(params) if params is not None else None,
        "dftype": constants.DfType.PANDAS,
    }
    if keystore_path is not None:
        kwargs["ks"] = keystore_path

    result = edsan_dbc.sqlserver_execute_query(
        constants.Database.ICCAJ,
        sql,
        **kwargs,
    )

    if result is None:
        raise RuntimeError(
            "d2im returned no dataframe. The Python keystore may not have "
            "unlocked, or the ICCAJ database configuration may be unavailable."
        )

    return result
