import sys
from datetime import datetime
import pandas as pd
from pdstats.data_import import DataImporter, DataSeries
from argparse import ArgumentParser
from pdcomponent.data_entity import DataEntity
from pdcomponent.customer import Customer
from pdcomponent.station import Station
from pdcomponent.device import Device


def main(filename):
    df = DataImporter.xls_import(filename)
    for idx, col in enumerate(df.columns):
        if col[0:7] != 'Unnamed':
            ds = DataSeries(df, idx, col)
            ds.analyze(2)
            ds.report()


def load_group_data(group_id, dt_begin, dt_end):
    from pdcomponent.device import Device
    dev = Device(group_id)
    dt_begin = datetime.strptime(dt_begin, '%Y-%m-%dT%H:%M:%S')
    dt_end = datetime.strptime(dt_end, '%Y-%m-%dT%H:%M:%S')
    if ((dt_begin-dt_end).days > 125) or ((dt_begin-dt_end).days < -125):
        raise RuntimeError("Invalid Date Range!")
    print(group_id, dt_begin, dt_end)
    dev.begin_datetime = dt_begin
    dev.ending_datetime = dt_end
    dev.load_trend_data()

    for idx, ch in enumerate(dev.gChannel):
        data = list()
        pdm: DataEntity
        data.append(["{}_CH{}".format(dev.gName, ch), "", ""])
        data.append(["timestamp", "Magnitude", "Count"])
        for row_count, pdm in enumerate(dev.trend_data.get(ch)):
            data.append([pdm.bdTime, pdm.bdMV, pdm.bdCount])
        df = pd.DataFrame(data)
        ds = DataSeries(df, 0, "{}_CH{}".format(dev.gName, ch))
        ds.gno, ds.sno, ds.sname = dev.gNo, dev.sNo_link, dev.sName
        ds.report()


if __name__ == "__main__":
    parser = ArgumentParser(add_help=True)
    group_file = parser.add_argument_group()
    group_file.add_argument("--file", help="loading xlsx file exported by PDSimply")

    group_exclude = parser.add_mutually_exclusive_group(required=True)
    group_exclude.add_argument("--gno", type=int, help="loading data from PUMO database by the specified group id")
    group_exclude.add_argument("--sno", type=int, help="loading data from PUMO database by the specified station id")
    group_exclude.add_argument("--cuno", type=int, help="loading data from PUMO database by the specified customer id")

    group_loading = parser.add_argument_group()
    group_loading.add_argument("--begin", type=str, help="begin datetime, max range 125 days., %Y-%m-%dT%H:%M:%S")
    group_loading.add_argument("--end", type=str, help="end datetime, %Y-%m-%dT%H:%M:%S")

    args = parser.parse_args()

    if args.gno is not None:
        load_group_data(group_id=args.gno, dt_begin=args.begin, dt_end=args.end)
        sys.exit()

    if args.sno is not None:
        st = Station(sNo=args.sno)
        dev: Device
        for dev in st.devices:
            load_group_data(group_id=dev.gNo, dt_begin=args.begin, dt_end=args.end)
        sys.exit()

    if args.cuno is not None:
        cu = Customer(cuNo=args.cuno)
        dev: Device
        for dev in cu.get_all_devices():
            load_group_data(group_id=dev.gNo, dt_begin=args.begin, dt_end=args.end)
        sys.exit()

    if args.file is not None:
        main(filename=args.file)
        sys.exit()

    parser.print_help()
