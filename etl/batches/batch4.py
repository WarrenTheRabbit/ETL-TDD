from automation.batch import Batch
import stage_policyholder_into_optimised 
import stage_provider_into_optimised 
import stage_date_into_optimised

def get_batch(**config):
    
   return Batch([
                  stage_policyholder_into_optimised, 
                  stage_provider_into_optimised, 
                  stage_date_into_optimised
               ], 
               **config)