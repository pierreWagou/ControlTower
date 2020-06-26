import textwrap
import datetime
import collections
from cassandra.cluster import Cluster
from calendar import monthrange
from flight_data import FlightData

class FeedCassandra:

    def __init__(self):
        self._session = Cluster().connect('romonpie_ks')
        self.fd = FlightData()
        self.DelayFlight = collections.namedtuple("DelayFlight", (
            "date",
            "tail_num",
            "delay"
        ))

    def filter(self, flight):
        """ filter a flight before insertion in cassandra databse
        Parameters
        ----------
        flight : Flight
            flight to be filtered
        """
        return not flight.cancelled and not flight.diverted

    def feed(self):
        """ feed cassandra databasec"""
        data = self.fd.read_data()
        # data = self.fd.limiter(self.fd.read_data())
        for flight in data:
            if self.filter(flight):
                self._insert_flight(flight)

    def _insert_flight(self, flight):
        """ insert a flight in cassandra databse
        Parameters
        ----------
        flight : Flight
            flight to be inserted
        """
        query = textwrap.dedent(f"""
            insert into time_flight (
                year,
                month,
                weekday,
                date,
                tail_num,
                delay
            ) values (
                {flight.date.year},
                {flight.date.month},
                {flight.date.weekday()},
                '{flight.date.isoformat()[:23]}',
                '{flight.plane.tail_num}',
                {flight.arr_delay}
            );
        """)
        self._session.execute(query)

    def get_month_weekday_flights(self, year, month, weekday):
        """ get flights for a weekday in a month from cassandra database
        Parameters
        ----------
        year : int
            year for the flights
        month : int
            month for the flights
        weekday : int
            weekday for the flights
        Returns
        -------
        Iterable
            selected flights
        """
        query = textwrap.dedent(f"""
            select *
            from time_flight
            where
                year={year} and
                month = {month} and
                weekday = {weekday}
        """)
        for row in self._session.execute(query):
            yield self.DelayFlight(row.date, row.tail_num, row.delay)

    def get_year_weekday_flights(self, year, weekday, max_delay=1000):
        """ get flights for a weekday in a year from cassandra database
        Parameters
        ----------
        year : int
            year for the flights
        weekday : int
            weekday for the flights
        max_delay : int
            delay max for the flights
        Returns
        -------
        Iterable
            selected flights
        """
        for month in range(1, 13):
            for flight in self.get_month_weekday_flights(year, month, weekday):
                if abs(flight.delay)<max_delay:
                    yield flight

    def get_month_flights(self, year, month):
        """ get flights for a month from cassandra database
        Parameters
        ----------
        year : int
            year for the flights
        month : int
            month for the flights
        Returns
        -------
        Iterable
            selected flights
        """
        for weekday in range(7):
            for flight in self.get_month_weekday_flights(year, month, weekday):
                yield flight

    def get_year_flights(self, year):
        """ get flights for a year from cassandra database
        Parameters
        ----------
        year : int
            year for the flights
        Returns
        -------
        Iterable
            selected flights
        """
        for weekday in range(7):
            for flight in self.get_year_weekday_flights(year, weekday):
                yield flight
