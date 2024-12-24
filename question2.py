from mrjob.job import MRJob
from mrjob.step import MRStep

class NombreVentes(MRJob):
    def mapper(self, _, line):
        if not line.startswith('ORDERNUMBER'):
            try:
                fields = line.strip().split(',')
                product_line = fields[10]
                quantity =int(fields[1]) 
                if product_line and quantity:  
                    yield product_line, quantity
            except:
                pass  

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    NombreVentes.run()
