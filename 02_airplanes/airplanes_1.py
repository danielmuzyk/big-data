from mrjob.job import MRJob


class FlightsDistance(MRJob):

    def mapper(self, _, line):

        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT,
         SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME,
         AIR_TIME,DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED,
         CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) = line.split(',')

        MONTH, DAY, DISTANCE = int(MONTH), int(DAY), int(DISTANCE)
        yield YEAR, (MONTH, DAY, AIRLINE, DISTANCE)

# Decreasing size of base file called flights.csv, use command:
# python airplanes_1.py flights.csv > preprocessed_data.csv
# to create new file


if __name__ == "__main__":
    FlightsDistance.run()