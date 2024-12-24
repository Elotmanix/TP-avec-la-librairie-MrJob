from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
class ChiffreAffaire(MRJob):
    def mapper(self, _, line):
        if not line.startswith('ORDERNUMBER'):
            try:
                fields = line.strip().split(',')
                product_line = fields[10]
                quantity =int(fields[1])  
                price = float(fields[2])   
                date_str = fields[5]       
                
                date =datetime.strptime(date_str, '%m/%d/%Y %H:%M')
                year = date.year
                
                if product_line and quantity and price:
                    revenue =quantity * price
                    yield (year, product_line), revenue
            except:
                pass  

    def reducer(self, key, values):
        yield f"{key[0]} - {key[1]}",sum(values)

if __name__ == '__main__':
    ChiffreAffaire.run()
