from automation.batch import Batch

def get_batch(**config):
    
   return Batch([
                  stage_claim_into_raw, 
                  stage_policyholder_into_raw, 
                  stage_provider_into_raw
               ], 
               **config)