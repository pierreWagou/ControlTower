import collections
import datetime
import pyspark
from flight_data import FlightData

class GetRdd:

    def __init__(self):
        self.fd = FlightData()
        self.DelayPlane = collections.namedtuple("DelayPlane", (
            "date",
            "issue_date",
            "delay",
        ))

    def is_complete(self, flight):
        """ check data integrity
        Parameters
        ----------
        flight : Flight
            flight to be checked
        Returns
        -------
        bool
            integrity of the flight
        """
        return  flight.plane.issue_date!=None and flight.plane.issue_date!='None'

    def filter(self, flight, max=10000):
        """ filter datas
        Parameters
        ----------
        flight : Flight
            flight to be filtered
        Returns
        -------
        bool
            acceptance of the flight
        """
        return not flight.cancelled and not flight.diverted and abs(flight.arr_delay)<=max

    def get_data(self, max_delay):
        """ transpose datas
        Parameters
        ----------
        max_delay : int
            delay maximum for the flights
        """
        # for flight in self.fd.limiter(self.fd.read_data("2008.csv"), 10000):
        for flight in self.fd.read_data("2008.csv"):
            if self.is_complete(flight) and self.filter(flight, max_delay):
                issue_date = datetime.datetime.strptime(flight.plane.issue_date, '%m/%d/%Y')
                yield self.DelayPlane(
                    flight.date,
                    issue_date,
                    flight.arr_delay
                )

    def get_rdd(self, max_delay):
        """ configure pyspark
        Parameters
        ----------
        max_delay : int
            delay maximuù for the flights
        Returns
        -------
        RDD
            parallelize datas
        """
        sparkconf = pyspark.SparkConf()
        sparkconf.set('spark.port.maxRetries', 128)
        sc = pyspark.SparkContext(conf=sparkconf)
        sc.setLogLevel("OFF")
        rdd = sc.parallelize(self.get_data(max_delay))
        return rdd
