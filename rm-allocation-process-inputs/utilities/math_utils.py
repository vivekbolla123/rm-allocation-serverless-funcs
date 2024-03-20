import math


class MathUtils:
    def __init__(self, inputsource):
        self.inputSource = inputsource

    def generateRoundNumber(self, number):
        fractional_part = number - int(number)
        if fractional_part >= 0.5:
            return int(number) + 1
        elif fractional_part < 0.5:
            return int(number)

    def dynamicAllocationD3Formula(self, dPLF, remainingCapacity, remainingRBD, ndocounter):
        return int(round((remainingCapacity/remainingRBD) * (1/(1+(dPLF/100)+0.001)) / math.exp(3*(dPLF/100)/((ndocounter/75)+0.1)), 0))

    def dynamicAllocationD4Formula(self, dPLF, rbdPosition, d3Allocation):
        return int(round(d3Allocation * math.exp(((dPLF/100)*rbdPosition)/3), 0))

    def generateMaxValue(self, remainingCapacity, ndocounter):
        return int(round(max((remainingCapacity/(1+ndocounter)), 15), 0))

    def getCeilNumber(self, number):
        return math.ceil(number)

    def calculateLogValueForD3(self, maxRBDPrice, openingRBDPirce, jumpPercent):
        return math.log(maxRBDPrice/openingRBDPirce, (1+jumpPercent/100))
