import collections
import csv
import datetime

class FlightData:

    def __init__(self):
        self.planes = self.read_file("plane-data.csv")
        self.Plane = collections.namedtuple("Plane", (
            "tail_num",
            "type",
            "manufacturer",
            "issue_date",
            "model",
            "status",
            "aircraft_type",
            "engine_type",
            "year"
        ))
        self.Flight = collections.namedtuple("Flight", (
            "date",
            "dep_time",
            "crs_dep_time",
            "arr_time",
            "crs_arr_time",
            "carrier",
            "flight_num",
            "plane",
            "elapsed_time",
            "crs_elapsed_time",
            "air_time",
            "arr_delay",
            "dep_delay",
            "origin",
            "dest",
            "distance",
            "taxi_in",
            "taxi_out",
            "cancelled",
            "cancellation_code",
            "diverted",
            "carrier_delay",
            "weather_delay",
            "nas_delay",
            "security_delay",
            "late_aircraft_delay"
        ))

    def limiter(self, generator, limit):
        """ limit generator size for testing phase
        Parameters
        ----------
        generator : Iterable
            Iterable to be limited
        limit : int
            limit on the generator
        Returns
        -------
        Iterable
            limited generator
        """
        return (data for _, data in zip(range(limit), generator))

    def is_complete(self, row):
        """ check data integrity
        Parameters
        ----------
        row : Dictionary
            row to be checked
        Returns
        -------
        bool
            integrity of the row
        """
        if row["CRSDepTime"]=='NA':
            return False
        else:
            return True

    def complete_row(self, row):
        """ complete incomplete row
        Parameters
        ----------
        row : Dictionary
            row to be completed
        """
        for key in row.keys():
            if row[key]=="NA" or row[key]=="":
                switcher = {
                    "DepTime": row["CRSDepTime"],
                    "ArrTime": row["CRSArrTime"],
                    "Cancelled": '0',
                    "Diverted": '0'
                }
                row[key] = switcher.get(key, 0)

    def convert_hour(self, row):
        """ convert hour format of a flight
        Parameters
        ----------
        row : Dictionary
            row whose hour is converted
        Returns
        -------
        tuple
            tuple with time (hour, minute)
        """
        if len(row["CRSDepTime"])<4:
            time_str = ''.join('0' for _ in range(len(row["CRSDepTime"]), 4)) + row["CRSDepTime"]
        else:
            time_str = str(row["CRSDepTime"])
        if time_str[0:2]=='24':
            time_str = time_str.replace(time_str[0:2], '00', 1)
        hour = datetime.datetime.strptime(time_str, '%H%M').hour
        minute = datetime.datetime.strptime(time_str, '%H%M').minute
        return hour, minute

    def read_file(self, file_name):
        """ read secondary CSV file
        Parameters
        ----------
        file_name : str
            name of the file to be read
        Returns
        -------
        Iterable
            rows of the CSV file
        """
        with open(file_name) as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
        return data

    def find_plane(self, tail_num):
        """ find plane for a flight from tail num
        Parameters
        ----------
        tail_num : str
            tail num for the flight
        Returns
        -------
        Plane
            plane corresponding to the flight
        """
        for plane in self.planes:
            if plane["tailnum"]==tail_num:
                return self.Plane(
                    plane["tailnum"],
                    plane["type"],
                    plane["manufacturer"],
                    plane["issue_date"],
                    plane["model"],
                    plane["status"],
                    plane["aircraft_type"],
                    plane["engine_type"],
                    plane["year"],
                )

    def read_data(self, file_name="2000.csv"):
        """ read main CSV file
        Parameters
        ----------
        file_name : str
            name of the file to be read
        Returns
        -------
        Iterable
            rows of the CSV file
        """
        with open(file_name) as file:
            reader = csv.DictReader(file)
            for row in reader:
                plane = self.find_plane(row["TailNum"])
                if plane!=None and self.is_complete(row):
                    self.complete_row(row)
                    hour, minute = self.convert_hour(row)
                    date = datetime.datetime(int(row["Year"]), int(row["Month"]), int(row["DayofMonth"]), hour, minute)
                    cancelled = row["Cancelled"]=='1'
                    diverted = row["Diverted"]=='1'
                    yield self.Flight(
                        date,
                        row["DepTime"],
                        row["CRSDepTime"],
                        row["ArrTime"],
                        row["CRSArrTime"],
                        row["UniqueCarrier"],
                        int(row["FlightNum"]),
                        plane,
                        row["ActualElapsedTime"],
                        row["CRSElapsedTime"],
                        row["AirTime"],
                        int(row["ArrDelay"]),
                        row["DepDelay"],
                        row["Origin"],
                        row["Dest"],
                        row["Distance"],
                        row["TaxiIn"],
                        row["TaxiOut"],
                        cancelled,
                        row["CancellationCode"],
                        diverted,
                        row["CarrierDelay"],
                        row["WeatherDelay"],
                        row["NASDelay"],
                        row["SecurityDelay"],
                        row["LateAircraftDelay"]
                    )
