from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class AverageOrdersByDayCategory(MRJob):
    """
    cette requête : calculer le moyenne des quantités commandées chaque jour de la semaine 
    pour chaque catégorie de produits,pour identifier les tendances de vente 
    selon les jours de la semaine.

    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_orders,
                  reducer=self.reducer_sum_orders),
            MRStep(reducer=self.reducer_calculate_average)
        ]

    def mapper_get_orders(self, _, line):
        if not line.startswith('ORDERNUMBER'):
            try:
                fields = line.strip().split(',')
                product_line = fields[10]
                quantity = int(fields[1])
                date_str = fields[5]
                # Convertir la date en jour de la senaine
                date = datetime.strptime(date_str, '%m/%d/%Y %H:%M')
                day_of_week = date.strftime('%A')
                
                if all([product_line, quantity]):
                    yield (product_line, day_of_week), (quantity, 1)

            except:
                pass

    def reducer_sum_orders(self, key, values):
        total_quantity = 0
        total_orders = 0
        for quantity, count in values:
            total_quantity += quantity
            total_orders += count
        yield key[0], (key[1], total_quantity, total_orders)

    def reducer_calculate_average(self, category, day_totals):
        day_averages = {}
        for day, total_quantity, total_orders in day_totals:
            average = total_quantity / total_orders
            day_averages[day] = average
        yield category, day_averages




if __name__ == '__main__':
    AverageOrdersByDayCategory.run()
