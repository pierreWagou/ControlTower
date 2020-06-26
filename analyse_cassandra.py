import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from functools import reduce
import calendar
from feed_cassandra import FeedCassandra

class AnalyseCassandra:

    def __init__(self):
        self.fc = FeedCassandra()
        sns.set(rc={'figure.figsize':(20, 10)})

    def _compute_avg(self, gen):
        """ compute average of datas from a generator
        Parameters
        ----------
        gen : Iterable
            datas to be used for average calculation
        Returns
        -------
        float
            average of the datas
        """
        tuple_map = map(lambda f: (1, f.delay), gen)
        sum_reduce = reduce(lambda x, y: (x[0]+y[0], x[1]+y[1]), tuple_map)
        avg = sum_reduce[1] / sum_reduce[0]
        return avg

    def _compute_month_avg(self, year, month):
        """ compute average delay for a month
        Parameters
        ----------
        year : int
            year of the flights
        month : int
            month of the flights
        Returns
        -------
        float
            average of the month
        """
        return self._compute_avg(self.fc.get_month_flights(year, month))

    def plot_weekday_flights(self, year, weekday):
        """ plot average by hour for a weekday
        Parameters
        ----------
        year : int
            year of the flights
        weekday : int
            weekday of the flights
        """
        delays = [0 for _ in range(24)]
        nbs = [0 for _ in range(24)]
        for flight in self.fc.get_year_weekday_flights(year, weekday):
            delays[flight.date.hour] += flight.delay
            nbs[flight.date.hour] += 1
        delays = [delay/nb if nb>0 else 0 for delay, nb in zip(delays, nbs)]
        plot = sns.barplot(x=list(range(24)), y=delays)
        plot.set(xlabel="Days", ylabel="Delay (minute)")
        plot.set_title(f"Average delay by day for {calendar.day_name[weekday]}")
        plt.savefig(f"delay_weekday.png")
        plt.clf()

    def plot_month_flights(self, year, month):
        """ plot average by day for a month
        Parameters
        ----------
        year : int
            year of the flights
        month : int
            month of the flights
        """
        month_days = range(1, calendar.monthrange(year, month)[1]+1)
        delays = [0 for _ in month_days]
        nbs= [0 for _ in month_days]
        for flight in self.fc.get_month_flights(year, month):
            delays[flight.date.day-1] += flight.delay
            nbs[flight.date.day-1] += 1
        delays = [delay/nb if nb>0 else 0 for delay, nb in zip(delays, nbs)]
        plot = sns.barplot(x=list(month_days), y=delays)
        plot.set(xlabel="Days", ylabel="Delay (minute)")
        plot.set_title(f"Average delay by day for {calendar.month_name[month]}")
        plt.savefig(f"delay_month.png")
        plt.clf()

    def plot_year_flights(self, year):
        """ plot average by month for a year
        Parameters
        ----------
        year : int
            year of the flights
        """
        delays = [self._compute_month_avg(year, month) for month in range(1, 13)]
        month_names = [calendar.month_name[month] for month in range(1, 13)]
        plot = sns.barplot(x=month_names, y=delays)
        plot.set(xlabel="Months", ylabel="Delay (minute)")
        plot.set_title(f"Average delay by month for {year}")
        plt.savefig("delay_year.png")
        plt.clf()

    # use fractile approximation here

    # def plot_week_flights(self, year):
    #     """ plot average by weekday for a week
    #     Parameters
    #     ----------
    #     year : int
    #         year of the flights
    #     """
    #     for weekday in range(7):
    #         delays = [flight.delay for flight in self.fc.get_year_weekday_flights(year, weekday, 60)]
    #         plot = sns.distplot(delays, hist=False, label=f"{calendar.day_name[weekday]}")
    #     plot.set(xlabel="Delay (minute)")
    #     plot.set_title("distplot")
    #     plt.savefig("delay_week.png")
    #     plt.clf()
