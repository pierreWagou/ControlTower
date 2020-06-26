import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from scipy import interpolate
from dateutil.relativedelta import relativedelta
from get_rdd import GetRdd

class AnalyseSpark:

    def __init__(self, max_delay):
        sns.set(rc={'figure.figsize':(20, 10)})
        self.rdd = GetRdd().get_rdd(max_delay)

    def compute_avg(self, key_function, value_function):
        """ Generalize computation of average from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        Returns
        -------
        Iterable
            tuples with (key, average)
        """
        map = self.rdd.map(lambda t: (key_function(t), (1, value_function(t))))
        reduce = map.reduceByKey(lambda x, y: add_tuples(x, y))
        map = reduce.map(lambda t: get_avg(t))
        sort = map.sortByKey(ascending=True)
        return sort.collect()

    def compute_dev(self, key_function, value_function):
        """ Generalize computation of deviation from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        Returns
        -------
        Iterable
            tuples with (key, deviation)
        """
        map = self.rdd.map(lambda t: (key_function(t), (1, value_function(t), value_function(t)**2)))
        reduce = map.reduceByKey(lambda x, y: add_tuples(x, y))
        map = reduce.map(lambda t: get_dev(t))
        sort = map.sortByKey(ascending=True)
        return sort.collect()

    def compute_skew(self, key_function, value_function):
        """ Generalize computation of skewness from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        Returns
        -------
        Iterable
            tuples with (key, skewness)
        """
        map = self.rdd.map(lambda t: (key_function(t), (1, value_function(t), value_function(t)**2, value_function(t)**3)))
        reduce = map.reduceByKey(lambda x, y: add_tuples(x, y))
        map = reduce.map(lambda t: get_skew(t))
        sort = map.sortByKey(ascending=True)
        return sort.collect()

    def compute_kurt(self, key_function, value_function):
        """ Generalize computation of kurtosis from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        Returns
        -------
        Iterable
            tuples with (key, kurtosis)
        """
        map = self.rdd.map(lambda t: (key_function(t), (1, value_function(t), value_function(t)**2, value_function(t)**3, value_function(t)**4)))
        reduce = map.reduceByKey(lambda x, y: add_tuples(x, y))
        map = reduce.map(lambda t: get_kurt(t))
        sort = map.sortByKey(ascending=True)
        return sort.collect()

    def compute_measures(self, key_function, value_function):
        """ Generalize computation of measures from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        Returns
        -------
        Iterable
            tuples with (key, (average, deviation, skewness, kurtosis))
        """
        map = self.rdd.map(lambda t: (key_function(t), (1, value_function(t), value_function(t)**2, value_function(t)**3, value_function(t)**4)))
        reduce = map.reduceByKey(lambda x, y: add_tuples(x, y))
        map = reduce.map(lambda t: get_measures(t))
        sort = map.sortByKey(ascending=True)
        return sort.collect()

    def plot_measures(self, key_function, value_function):
        """ Plot measures from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys
        value_function : function
            function returning values
        """
        measures = self.compute_measures(key_function, value_function)
        keys = [key for key, _ in measures]
        avgs = [avg for key, (avg, _, _, _) in measures]
        plot = sns.barplot(x=keys, y=avgs)
        plot.set(xlabel="Key", ylabel="Average")
        plot.set_title("Graph of values averages by keys")
        plt.savefig("avg_plot.png")
        plt.clf()
        devs = [dev for key, (_, dev, _, _) in measures]
        plot = sns.barplot(x=keys, y=devs)
        plot.set(xlabel="Key", ylabel="Deviation")
        plot.set_title("Graph of values deviation by keys")
        plt.savefig("dev_plot.png")
        plt.clf()
        skews = [skew for key, (_, _, skew, _) in measures]
        plot = sns.barplot(x=keys, y=skews)
        plot.set(xlabel="Key", ylabel="Skewness")
        plot.set_title("Graph of values skewness by keys")
        plt.savefig("skew_plot.png")
        plt.clf()
        kurts = [kurt for key, (_, _, _, kurt) in measures]
        plot = sns.barplot(x=keys, y=kurts)
        plot.set(xlabel="Key", ylabel="Kurtosis")
        plot.set_title("Graph of values kurtosis by keys")
        plt.savefig("kurt_plot.png")
        plt.clf()

    def pearson_test(self, key_function, value_function):
        """ Compute links between distance average and hours with pearson
        Returns
        -------
        Iterable
            tuples with (likehood, p-value)
        """
        datas = self.compute_avg(key_function, value_function)
        x = [r[0] for r in datas]
        y = [r[1] for r in datas]
        return pearsonr(x,y)

    def compute_min_max(self, key_function, value_function):
        """ Generalize computation of min and max from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        Returns
        -------
        Iterable
            tuples with (key, (min, max))
        """
        map = self.rdd.map(lambda t: (key_function(t), (value_function(t), value_function(t))))
        reduce = map.reduceByKey(lambda x, y: (min(x[0], y[0]), max(x[1], y[1])))
        sort = reduce.sortByKey(ascending=True)
        return sort.collect()

    def compute_distrib(self, key_function, value_function, propos):
        """ Generalize computation of distribution from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        Returns
        -------
        Iterable
            tuples with (key, ([(x, y)]))
        """
        map = self.rdd.map(lambda t: get_distrib(t, key_function, value_function, propos))
        reduce = map.reduceByKey(lambda x, y: (x[0], add_tuples(x[1], y[1])))
        map = reduce.map(lambda t: get_point(t))
        sort = map.sortByKey(ascending=True)
        return sort.collect()

    def fractile(self, key_function, value_function, propo_function, nb_fractiles):
        """ Generalize computation of fractiles from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        propo_function : function
            function for the interpolation
        nb_fractiles : int
            number of fractiles
        Returns
        -------
        Iterable
            tuples with (key, ([(x, y)]))
        """
        alphas = [i/nb_fractiles for i in range(1, nb_fractiles)]
        min_max = self.compute_min_max(key_function, value_function)
        points = {key: {(min, 0), (max, 1)} for key, (min, max) in min_max}
        iteration = 0
        updating = True
        while updating:
            iteration+=1
            updating = False
            propos = {}
            for key in points:
                propos[key] = propo_function(points[key], alphas)
            distrib = self.compute_distrib(key_function, value_function, propos)
            for key, values in distrib:
                for value, alpha in zip(values, alphas):
                    updating = updating or (abs(value[1]-alpha)>0.1)
                    points[key].add(value)
        for i in range(len(min_max)):
            distrib[i][1].insert(0,(min_max[i][1][0], 0))
            distrib[i][1].append((min_max[i][1][1], 1))
        return distrib

    def lin_fractile(self, key_function, value_function, nb_fractiles):
        """ Generalize computation of fractiles with linear interpolation from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        nb_fractiles : int
            number of fractiles
        Returns
        -------
        Iterable
            tuples with (key, ([(x, y)]))
        """
        return self.fractile(key_function, value_function, linpropo, nb_fractiles)

    def cub_fractile(self, key_function, value_function, nb_fractiles):
        """ Generalize computation of fractiles with cube spline interpolation from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        nb_fractiles : int
            number of fractiles
            function for the interpolation
        Returns
        -------
        Iterable
            tuples with (key, ([(x, y)]))
        """
        return self.fractile(key_function, value_function, cubpropo, nb_fractiles)

    def plot_fractile(self, key_function, value_function, nb_fractiles, fractile_function):
        """ Plot fractiles from user defined key and values functions
        Parameters
        ----------
        key_function : function
            function returning keys to map
        value_function : function
            function returning values to be mapped
        nb_fractiles : int
            number of fractiles
        fractile_function : function
            function for interpolation
        Returns
        -------
        Iterable
            tuples with (key, ([(x, y)]))
        """
        fractiles = fractile_function(key_function, value_function, nb_fractiles)
        df = pd.DataFrame(columns=[key for key, _ in fractiles])
        for key, values in fractiles:
            column = [value[0] for value in values]
            df[key] = column
        df = pd.melt(df)
        plot = sns.boxplot(x="variable", y="value", data=df)
        plot.set(xlabel="Key", ylabel="Value")
        plot.set_title("Box plot")
        plt.savefig("box_plot.png")
        plt.clf()

    # ----------------
    # Helper functions
    # ----------------

def get_tail_num(plane):
    return plane.tail_num

def get_age(plane):
    return relativedelta(plane.date, plane.issue_date).years

def get_delay(plane):
    return plane.delay

def add_tuples(t1, t2):
    """ Add two tuples component by component
    Parameters
    ----------
    t1 : tuple
        first tuple for the addition
    t2 : tuple
        second tuple for the addition
    Returns
    -------
    tuple
        tuple with the sum of the two tuples components
    """
    t = [c1+c2 for c1, c2 in zip(t1, t2)]
    return t

def get_avg(t):
    """ Compute average from a specific tuple
    Parameters
    ----------
    t : tuple
        tuple with (key (number, sum))
    Returns
    -------
    tuple
        tuple with (key, average)
    """
    k, (n, s) = t
    avg = s / n
    return k, avg

def get_dev(t):
    """ Compute deviation from a specific tuple
    Parameters
    ----------
    t : tuple
        tuple with (key (number, sum, sum**2))
    Returns
    -------
    tuple
        tuple with (key, deviation)
    """
    k, (n, s, s2) = t
    _, avg = get_avg((k, (n, s)))
    _, avg2 = get_avg((k, (n, s2)))
    dev = np.sqrt(avg2 - avg**2)
    return k, dev

def get_skew(t):
    """ Compute skewness from a specific tuple
    Parameters
    ----------
    t : tuple
        tuple with (key (number, sum, sum**2, sum**3))
    Returns
    -------
    tuple
        tuple with (key, skewness)
    """
    k, (n, s, s2, s3) = t
    _, avg = get_avg((k, (n, s)))
    skew = ((s3 - 3*avg*s2 + 3*s*avg**2 - n*avg**3)/n) / ((s2 - 2*s*avg + n*avg**2)/(n-1))**(3/2)
    return k, skew

def get_kurt(t):
    """ Compute kurtosis from a specific tuple
    Parameters
    ----------
    t : tuple
        tuple with (key (number, sum, sum**2, sum**3, sum**4))
    Returns
    -------
    tuple
        tuple with (key, kurtosis)
    """
    k, (n, s, s2, s3, s4) = t
    _, avg = get_avg((k, (n, s)))
    kurt = ((s4 - 4*s3*avg + 6*s2*avg**2 - 4*s*avg**3 + n*avg**4)/n) / ((s2 - 2*s*avg + n*avg**2)/(n-1))**2
    return k, kurt

def get_measures(t):
    """ Compute average, deviation, skewness and kurtosis from a specific tuple
    Parameters
    ----------
    t : tuple
        tuple with (key (number, sum, sum**2, sum**3, sum**4))
    Returns
    -------
    tuple
        tuple with (key, (average, deviation, skewness, kurtosis))
    """
    k, (n, s, s2, s3, s4) = t
    _, avg = get_avg((k, (n, s)))
    _, dev = get_dev((k, (n, s, s2)))
    _, skew = get_skew((k, (n, s, s2, s3)))
    _, kurt = get_kurt((k, (n, s, s2, s3, s4)))
    return k, (avg, dev, skew, kurt)

def get_distrib(trip, key_function, value_function, propos):
    """ Compute proposed distribution from user defined key and values functions
    Parameters
    ----------
    trip : Trip
        trip to be distributed
    key_function : function
        function returning keys to map
    value_function : function
        function returning values to be mapped
    propos : dict
        proposed fractiles for each key
    Returns
    -------
    Iterable
        tuples with (key, (proposes fractiles for key, distribution))
    """
    key = key_function(trip)
    value = value_function(trip)
    distrib = (1,)
    for propo in propos[key]:
        distrib += ((value<=propo)*1,)
    return key, (propos[key], distrib)

def get_point(t):
    """ Compute fequences and gather it with value key and values functions
    Parameters
    ----------
    t : tuple
        tuple with (key, propos, frequence)
    Returns
    -------
    Iterable
        tuples with (key, [(value, frequence)])
    """
    key, (propos, freqs) = t
    point = key, [(propo, freq/freqs[0]) for propo, freq in zip(propos, freqs[1:])]
    return point


def _dicho_croissante(f, y, a, b):
    """ Search by dichotomy for the interpolation
    Parameters
    ----------
    f : function
        function for the interpolation
    y : float
        fractile
    a : float
        minimum value
    b : float
        maximum value
    Returns
    -------
    float
        result of the dichotomic search
    """
    c = (a + b) / 2
    while c != a and c != b:
        if f(c) < y:
            a = c
        else:
            b = c
        c = (a + b) / 2
    return c

def linpropo(points, alphas):
    """ Compute linear interpolation
    Parameters
    ----------
    points : set
        points to be interpolated
    fractiles : list
        fractiles to be estimated
    Returns
    -------
    Iterable
        estimation of fractiles
    """
    points = list(points)
    points.sort(key=lambda x: x[0])
    X = [x for x, y in points]
    Y = [y for x, y in points]
    interpol = interpolate.interp1d(X, Y)
    return [_dicho_croissante(interpol, alpha, X[0], X[-1]) for alpha in alphas]

def cubpropo(points, alphas):
    """ Compute cube spline interpolation
    Parameters
    ----------
    points : set
        points to be interpolated
    fractiles : list
        fractiles to be estimated
    Returns
    -------
    Iterable
        estimation of fractiles
    """
    points = list(points)
    points.sort(key=lambda x: x[0])
    X = [x for x, y in points]
    Y = [y for x, y in points]
    interpol = interpolate.PchipInterpolator(X, Y)
    return [_dicho_croissante(interpol, alpha, X[0], X[-1]) for alpha in alphas]
