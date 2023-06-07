from typing import List
import numpy as np

class Batch:
        
    def __init__(self, batch:List, **kwargs):
        self.jobs = batch
        self.output = None
        self.paths = None
        self.dfs = None
        self.kwargs = kwargs
        self.copy = None
        
    def run(self):
        self.output = [job.run(**self.kwargs) for job in self.jobs]
        self.dfs = np.array(self.output).flatten()[::2]
        self.paths = np.array(self.output).flatten()[1::2]
        
