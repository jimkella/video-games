from mrjob.job import MRJob
from mrjob.step import MRStep


class MrTop5Sales(MRJob):

    def steps(self):
        return[
            MRStep(mapper = self.mapping_games,
                   reducer = self.reduce_sales),
            MRStep(reducer = self.select_top5)    
    ]

    def mapping_games(self, _, lines):
        details = lines.split(",")
        yield details[0],float(details[9])
        
    def reduce_sales(self, key,values):
        yield None, (sum(values),key)
        
    def select_top5(self, _, pair):
        sorted_pairs = sorted(pair,reverse=True)
        for pair in sorted_pairs[0:5]:
            yield pair
        
if __name__ == '__main__':
    MrTop5Sales.run()