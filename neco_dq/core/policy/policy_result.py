# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from neco_dq.core.policy.exit_codes import DQExitCode


@dataclass
class DQPolicyResult:
    exit_code: DQExitCode
    message: str
