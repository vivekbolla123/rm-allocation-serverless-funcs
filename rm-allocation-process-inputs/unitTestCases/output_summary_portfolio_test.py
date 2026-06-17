"""Unit tests for the portfolio anchor split additions to ``OutputSummary`` and
``MarketListRow`` (Slice 3 of the ``portfolio-anchor-split`` spec).

Validates the R9 acceptance criteria — five new summary attributes
(``z_score_composite``, ``Portfolio_Anchor``, ``z_score88``,
``Portfolio88_Anchor``, ``portfolio_count``) default to ``CONST_NA``, and
``MarketListRow`` carries ``portfolio_count`` from the inbound message dict
(falling back to ``CONST_NA`` when not supplied — connections /
sales-inventory / legacy producers).
"""

import os
import sys
import unittest

# The test runner can be invoked from either the repo root or the Lambda
# folder — make sure both the Lambda's source files and the shared
# ``rm-allocation-configurations/`` package are importable.
HERE = os.path.dirname(os.path.abspath(__file__))
LAMBDA_ROOT = os.path.dirname(HERE)
REPO_ROOT = os.path.dirname(LAMBDA_ROOT)
SHARED_CONFIG = os.path.join(REPO_ROOT, "rm-allocation-configurations")
for path in (LAMBDA_ROOT, SHARED_CONFIG):
	if path not in sys.path:
		sys.path.insert(0, path)

from Constants import CONST_NA  # noqa: E402
from models.market_list_row import MarketListRow  # noqa: E402
from models.output_summary import OutputSummary  # noqa: E402


class TestOutputSummaryPortfolioDefaults(unittest.TestCase):
	"""R9.1 — five new summary attributes default to ``CONST_NA``."""

	def test_new_portfolio_attrs_default_to_const_na(self):
		row = OutputSummary()
		self.assertEqual(row.z_score_composite, CONST_NA)
		self.assertEqual(row.Portfolio_Anchor, CONST_NA)
		self.assertEqual(row.z_score88, CONST_NA)
		self.assertEqual(row.Portfolio88_Anchor, CONST_NA)
		self.assertEqual(row.portfolio_count, CONST_NA)


class TestMarketListRowPortfolioCount(unittest.TestCase):
	"""R13 — ``MarketListRow`` carries ``portfolio_count`` from the SQS body."""

	def test_portfolio_count_defaults_to_const_na_when_missing(self):
		row = MarketListRow({})
		self.assertEqual(row.portfolio_count, CONST_NA)

	def test_portfolio_count_picks_up_value_from_dict(self):
		row = MarketListRow({"portfolio_count": 5})
		self.assertEqual(row.portfolio_count, 5)


if __name__ == "__main__":
	unittest.main()
