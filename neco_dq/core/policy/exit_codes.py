# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from enum import IntEnum


class DQExitCode(IntEnum):
    SUCCESS = 0
    DQ_WARN = 10
    DQ_ERROR = 20
    SYSTEM_ERROR = 99
