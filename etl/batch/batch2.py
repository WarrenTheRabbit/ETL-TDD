from automation.batch import Batch
import stage_claim_into_access
import stage_policyholder_into_access
import stage_provider_into_access

def get_batch(**config):
    
   return Batch([
                  stage_claim_into_access, 
                  stage_policyholder_into_access, 
                  stage_provider_into_access
               ], 
                  **config)

