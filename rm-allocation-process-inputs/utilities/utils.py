import uuid


class Utils:
    def __init__(self,inputsource) :
        self.inputSource=inputsource
    
    def getUniqueId(self):
        return str(uuid.uuid4())
