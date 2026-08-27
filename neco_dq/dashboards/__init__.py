# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Dashboard provisioning for the DQ framework.

Generates Databricks AI/BI (Lakeview) dashboards from a versioned template
hydrated with a deployment's catalog / schema / history-table coordinates.

This is a **standalone** capability, deliberately not wired into ``run_dq()``:
dashboard provisioning is infrequent and needs workspace credentials that a
normal DQ run does not (see the design doc's rejected Option C).
"""

from neco_dq.dashboards.lakeview import generate_dashboard, hydrate_template

__all__ = ["generate_dashboard", "hydrate_template"]
