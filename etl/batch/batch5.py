from automation.batch import Batch
import stage_claim_into_optimised

def get_batch(**config):
    
   return Batch([stage_claim_into_optimised], 
               **config)