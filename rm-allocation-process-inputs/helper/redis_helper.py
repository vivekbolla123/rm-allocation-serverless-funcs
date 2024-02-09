from Constants import *
class RedisHelper:
    
    def __init__(self,runid,cache_client,input_source) :
        self.runId=runid
        self.cache_client=cache_client
        self.inputSource=input_source
        
    
    def updateRunAudit(self):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            self.cache_client.incr(KEY_B2B_DONE_COUNT + self.runId)
            self.cache_client.incr(KEY_B2C_DONE_COUNT + self.runId)
            
    def setAllRunStatus2Ready(self):
        b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + self.runId)
        b2cPendingCount = self.cache_client.get(KEY_B2C_PENDING_COUNT + self.runId)
        self.cache_client.set(KEY_B2B_READY_COUNT + self.runId, b2bPendingCount)
        self.cache_client.set(KEY_B2C_READY_COUNT + self.runId, b2cPendingCount)