from automation.batch import Batch
import stage_claim_into_raw
import stage_policyholder_into_raw
import stage_provider_into_raw

def get_batch(**config):
    
   return Batch([
                  stage_claim_into_raw, 
                  stage_policyholder_into_raw, 
                  stage_provider_into_raw
               ], 
               **config)