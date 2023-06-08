from automation.batch import Batch
import stage_location_into_optimised
import stage_procedure_into_optimised

def get_batch(**config):
    
   return Batch([
                  stage_location_into_optimised, 
                  stage_procedure_into_optimised
               ], 
               **config)
   