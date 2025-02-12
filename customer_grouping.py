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


class CustomerGrouping:
    cuNo: int = None
    cuName: str = None
    stations: dict = None
    devices: dict = None
    triggering: list = None

    def __init__(self, cuNo: int, cuName: str):
        self.stations = dict()
        self.devices = dict()
        self.cuNo = cuNo
        self.cuName = cuName
        self.triggering = []

    def append(self, tr: Triggering):
        self.triggering.append(tr)
        if tr.sName not in self.stations.keys():
            self.stations[tr.cuName + ', ' + tr.sName] = []

        self.stations[tr.cuName + ', ' + tr.sName].append(tr)
        condition_tag = (tr.cuName + ', ' + tr.sName + ', ' + tr.gName + ', CH' + str(tr.channel) + '_' +
                         str(tr.threshold_mv) + 'mv_' + str(tr.threshold_minutes) + 'min')
        
        if tr.gName not in self.devices.keys():
            self.devices[condition_tag] = []

        self.devices[condition_tag].append(tr)

    @staticmethod
    def csv_column_header(ym_tag_list: list):
        return ['dev'] + ym_tag_list

    def dump_csv_format(self):
        result = []
        trigger_list: list[Triggering]
        for tag, trigger_list in sorted(self.devices.items()):
            for trigger in trigger_list:
                result.append([trigger.gno, trigger.channel, trigger.tag()] +
                              [trigger.mean, trigger.std, trigger.threshold_mv, trigger.threshold_minutes] +
                              [len(dq) for dq in trigger.events_year_month.values()])

        return result

    def __str__(self):
        result = []
        for k, v in sorted(self.devices.items()):
            result.append('{}:{}'.format(k, v))

        return str.join('\n', result)

    def __repr__(self):
        return self.__str__()


def load_csv(csv_file: Path):
    import math
    from pdcomponent.core import MysqlOverviewEntity
    from pdcomponent import session

    # https://stackoverflow.com/a/57824142 當預設欄位太少不足以完成自動解析時  將會觸發記憶體不足 可以直接指定一個較大的數值
    column_names = [i for i in range(0, 2000)]
    rows = pd.read_csv(csv_file, header=None, delimiter=',', quotechar="'", names=column_names)
    tr_review = dict()

    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    tz = datetime.timezone(offset=datetime.timedelta(hours=8))
    tz_setter = pytz.timezone('Asia/Taipei')
    ym_tag_list: list[str] = []
    for idx, row in rows.iterrows():
        gno = int(row[0])
        channel_index = int(str(row[3]))
        t = Triggering(gno, channel_index, int(str(row[6])[:-2]), int(str(row[7])[:-3]),
                       mean=float(row[4]), std=float(row[5]))
        ts_begin = tz_setter.localize(datetime.datetime.strptime(str(row[1]), dt_fmt))
        ts_end = tz_setter.localize(datetime.datetime.strptime(str(row[2]), dt_fmt))
        ym_tag_list = list(t.build_year_month_bins(begin=ts_begin, end=ts_end))
        for event in row[8:]:
            if type(event) is float and math.isnan(event):
                continue
            # 2024-01-21T23:37:34
            t.append(datetime.datetime.strptime(event, dt_fmt))

        if gno not in tr_review.keys():
            tr_review['{}_{}'.format(gno, channel_index)] = []

        tr_review['{}_{}'.format(gno, channel_index)].append(t)

    cuno_grouping = dict()
    customer_info = [u.__dict__ for u in session.query(MysqlCustomer).all()]
    devices_overview = []
    for r in session.query(MysqlOverviewEntity).all():
        if r is not None:
            devices_overview.append(r.__dict__)

    for idx, tr_list in tr_review.items():
        tr: Triggering
        for tr in tr_list:  #if tr.check_over_triggering():
            gno = tr.gno
            dev_info = next(filter(lambda x: x['gNo'] == gno, devices_overview))
            tr.cuNo = dev_info['cuNo_link']
            cu_name = full2half(next(filter(lambda x: x['cuNo'] == tr.cuNo, customer_info))['cuName'])
            tr.cuName = cu_name
            tr.sName = full2half(dev_info['sName'])
            tr.gName = full2half(dev_info['gName'])

            if tr.cuNo not in cuno_grouping.keys():
                cuno_grouping[tr.cuNo] = CustomerGrouping(tr.cuNo, cu_name)

            grouping = cuno_grouping[tr.cuNo]
            grouping.append(tr)
            # print(tr)
    print("===================================================================================")
    header = CustomerGrouping.csv_column_header(ym_tag_list)
    csv_rows = [elem.dump_csv_format() for k, elem in cuno_grouping.items()]
    print(header + csv_rows)
    return header, csv_rows
