from mrjob.job import MRJob
from mrjob.step import MRStep

class TopStoreperCountry(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sales,
                  reducer=self.reducer_per_country_store),
            MRStep(reducer=self.reducer_find_top) ]

    def mapper_get_sales(self, _, line):
        if not line.startswith('ORDERNUMBER'):
            try:
                fields = line.strip().split(',')
                product_line = fields[10]
                if product_line == 'Trucks and Buses':
                    customer_name = fields[13]  
                    country = fields[20]       
                    quantity= int(fields[1])  
                    price = float(fields[2])   
                    if all([customer_name, country, quantity, price]):
                        revenue =quantity * price
                        yield (country, customer_name), revenue
            except:
                pass

    def reducer_per_country_store(self, key, values):
        yield key[0], (key[1], sum(values))

    def reducer_find_top(self, country, store_revenues):
        max_store = max(store_revenues, key=lambda x: x[1])
        yield country, max_store




if __name__ =='__main__':
    TopStoreperCountry.run()
