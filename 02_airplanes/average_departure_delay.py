from mrjob.job import MRJob
from mrjob.step import MRStep


class Delay(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):

        (year,month,day,day_of_week,airline,flight_number,tail_number,origin_airport,destination_airport,
        scheduled_departure,departure_time,departure_delay,taxi_out,wheels_off,scheduled_time,elapsed_time,air_time,
        distance,wheels_on,taxi_in,scheduled_arrival,arrival_time,arrival_delay,diverted,cancelled,cancellation_reason,
        air_system_delay,security_delay,airline_delay,late_aircraft_delay,weather_delay) = line.split(',')

        if departure_delay == '':
            departure_delay = '0'
        if airline_delay == '':
            airline_delay = '0'

        month = int(month)
        departure_delay = int(departure_delay)
        airline_delay = int(airline_delay)
        yield f'{month:02d}', (departure_delay, airline_delay)

    def reducer(self, key, values):
        total_dep_delay = 0
        total_arrival_delay = 0
        num_elements = 0
        for value in values:
            total_dep_delay += value[0]
            total_arrival_delay += value[1]
            num_elements += 1
        yield key, (total_dep_delay/num_elements, total_arrival_delay/num_elements)


if __name__ == "__main__":
    Delay.run()


#command:average_departure_delay.py flights.csv > average_delay.csv to create csv file