# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.COLUMN_CLASSIFICATION)
class TestColumnClassification:
    @allure.title("Classify columns by Spark type")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_classify_columns_mixed_types(self, profiling_suite, df_mixed):
        """Given a DataFrame with numeric, string, date, and boolean columns,
        when columns are classified, then each column is placed in the correct type bucket.
        """
        with allure.step("Classify a mixed-type DataFrame"):
            result = profiling_suite._classify_columns(df_mixed)

        with allure.step("Verify: classification matches observed buckets"):
            assert result == {
                "numeric": ["id", "amount"],
                "string": ["name"],
                "date": ["created"],
                "boolean": ["active"],
                "other": [],
            }
