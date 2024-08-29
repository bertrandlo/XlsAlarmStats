import datetime
from collections import deque
from pathlib import Path

import pandas as pd
import pytz

from dateutil.relativedelta import relativedelta

from pdcomponent.core import MysqlCustomer, full2half


class Triggering:
    cuNo: int = None
    cuName: str = None
    sName: str = None
    gName: str = None
    gno: int = None
    mean: float = None
    std: float = None
    channel: int = None
    threshold_mv: int = None
    threshold_minutes: int = None
    events_year_month: dict = None
    event_list: list = None

    def __init__(self, gno: int, channel: int, threshold_mv: int, threshold_minutes: int,
                 mean: float = None, std: float = None):
        self.gno = gno
        self.channel = channel
        self.threshold_mv = threshold_mv
        self.threshold_minutes = threshold_minutes
        self.events_year_month = dict()
        self.event_list = []
        self.mean = mean
        self.std = std

    def build_year_month_bins(self, begin: datetime.datetime, end: datetime.datetime):
        ts = datetime.datetime(begin.year, begin.month, 1, tzinfo=datetime.timezone(offset=datetime.timedelta(hours=8)))

        while True:
            key_year_month = ts.strftime("%Y-%m")
            self.events_year_month[key_year_month] = deque()
            ts = ts + relativedelta(months=1)
            if ts > end:
                break

        return self.events_year_month.keys()

    def append(self, ts: datetime.datetime):
        key_year_month = ts.strftime("%Y-%m")

        if ts not in self.event_list:
            self.event_list.append(ts)

        if key_year_month not in self.events_year_month.keys():
            self.events_year_month[key_year_month] = deque()

        dq = self.events_year_month[key_year_month]
        dq: deque
        dq.append(ts)

    def check_over_triggering(self) -> bool:
        for key, dq in self.events_year_month.items():
            if len(dq) > 50:
                return True

        return False

    def report(self):
        response = dict()
        for ym, dq in self.events_year_month.items():
            response[ym] = len(dq)
        return response

    def tag(self):
        r = ''
        if self.cuNo is not None:
            r = 'cuNo:{}({})'.format(self.cuNo, self.cuName)

        if self.sName is not None:
            r = r + ', ' + self.sName

        if self.gName is not None:
            r = r + ', ' + self.gName

        return '{}({})_CH{}'.format(r, self.gno, self.channel)

    def __str__(self):
        response = []
        for ym, dq in self.events_year_month.items():
            response.append('{}: #{}'.format(ym, len(dq)))

        return (self.tag() + ", {}mv, {} min: ".format(self.threshold_mv, self.threshold_minutes)
                + str.join(", ", response))

    def __repr__(self):
        return self.__str__()


