from typing import List
from automation.redshift import Redshift
import numpy as np

class Batch:
        
    def __init__(self, batch:List, **kwargs):
        self.batch = batch
        self.output = None
        self.paths = None
        self.dfs = None
        self.kwargs = kwargs
        self.copy = None
        
    def run(self):
        self.output = [job.run(**self.kwargs) for job in self.batch]
        self.dfs = np.array(self.output).flatten()[::2]
        self.paths = np.array(self.output).flatten()[1::2]