from mrjob.job import MRJob
from mrjob.step import MRStep


class CatégoriesProduits(MRJob) :
    def mapper(self, _ , line):
        if not line.startswith('ORDERNUMBER'):
            try:
                fields= line.strip().split(',')
                product_line=fields[10]
                if product_line :  
                    yield product_line, 1

            except:
                pass  

    def reducer(self, key, values):
        yield key, None

if __name__== '__main__':
    CatégoriesProduits.run()
