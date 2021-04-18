import datetime
import numpy as np
class Converter():
    
    @staticmethod
    def convertLetter(string):
        if isinstance(string, float):
            # If string is actually float of nan.
            return string
        string = string.replace(",", "")
        number = string[0:-1]
        letter = string[-1]
        if letter == "B":
            return float(number) * 1000
        if letter == "M":
            return float(number) 
        if letter == "k":
            return float(number) / 100

    @staticmethod
    def convertPercent(string):
        if isinstance(string, float):
            # If string is actually float of nan.
            return string
        if string == "∞%":
            return np.inf
        if string == "0":
            return 0
        string = string.replace(",", "")
        number = string[0:-1]
        return float(number)

    @staticmethod
    def convertDate(string):
        date = datetime.datetime.strptime(string, "%d %b %Y")
        return date

    @staticmethod
    def tryConvertToFloat(x):
        try:
            return float(x)
        except(ValueError, TypeError):
            return x    