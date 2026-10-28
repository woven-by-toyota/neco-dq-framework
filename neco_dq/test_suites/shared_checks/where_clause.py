# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Row-filter helper shared by checks that honour ``row_filter``."""


class WhereClauseMixin:
    """Provides ``_get_where_clause`` from the host suite's ``_current_where_clause``."""

    def _get_where_clause(self) -> str:
        """Get WHERE clause from row_filter if set, empty string otherwise."""
        row_filter = getattr(self, "_current_where_clause", None)
        return f" WHERE {row_filter}" if row_filter else ""
