# -*- coding: utf-8 -*-
from pdstats.data_import import DataImporter, DataSeries
from argparse import ArgumentParser


def main(filename):
    df = DataImporter.xls_import(filename)
    for idx, col in enumerate(df.columns):
        if col[0:7] != 'Unnamed':
            ds = DataSeries(df, idx, col)
            ds.analyze(2)
            ds.report()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("xlsx_filename", help="loading xlsx file exported by PDSimply")
    args = parser.parse_args()
    main(filename=args.xlsx_filename)
