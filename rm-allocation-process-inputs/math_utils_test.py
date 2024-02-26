import unittest
from utilities.math_utils import MathUtils
class TestMathUtils(unittest.TestCase):

    def setUp(self):
        # Initialize MathUtils object with a sample input source
        self.math_utils = MathUtils(inputsource='sample')

    def test_generateRoundNumber(self):
        self.assertEqual(self.math_utils.generateRoundNumber(5.4), 5)
        self.assertEqual(self.math_utils.generateRoundNumber(7.8), 8)
        self.assertEqual(self.math_utils.generateRoundNumber(3.2), 3)

    def test_dynamicAllocationD3Formula(self):
        result = self.math_utils.dynamicAllocationD3Formula(dPLF=0.5, remainingCapacity=100, remainingRBD=20, ndocounter=50)
        self.assertEqual(result, 0)

    def test_dynamicAllocationD4Formula(self):
        result = self.math_utils.dynamicAllocationD4Formula(dPLF=0.3, rbdPosition=2, d3Allocation=150)
        self.assertEqual(result, 183)

    def test_generateMaxValue(self):
        result = self.math_utils.generateMaxValue(200, ndocounter=30)
        self.assertEqual(result, 15)

if __name__ == '__main__':
    unittest.main()
