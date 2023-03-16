# -*- coding: utf-8 -*-
import unittest
from pdstats.data_import import DataImporter, DataSeries
import pandas as pd
from datetime import datetime
import numpy as np
from time import sleep


class TestDataSeries(unittest.TestCase):

    def test_read_ini_config(self):
        import configparser

        config = configparser.ConfigParser()
        max_voltage = config.read('test_config.ini')
        assert len(max_voltage) == 0

    def test_datetime_convert(self):
        df = DataImporter.xls_import('SampleData.xlsx')
        for idx, col in enumerate(df.columns):
            if col[0:7] != 'Unnamed':
                ds = DataSeries(df, idx, col)
                print(ds.dt)

    def test_clean_series_endpoint(self):
        df = DataImporter.xls_import('SampleData.xlsx')
        for idx, col in enumerate(df.columns):
            if col[0:7] != 'Unnamed':
                ds = DataSeries(df, idx, col)
                threshold = ds.get_voltage_threshold(2)
                print("{} Threshold: {}".format(ds.device_name, threshold))
                ds.alarm_array = np.where(ds.voltage >= threshold, 1, 0)  # 將高於門檻值標示成 1 否則為 0
                ds.occurrence = np.diff(ds.alarm_array)          # 清理轉換成差值 n+1 - n => 0: (0->0, 1->1),  1: (0->1),  -1: (1->0)
                ds.occurrence_node = ds.occurrence.nonzero()[0]  # 取得非零元素的索引形成 tuple
                ds.clean_series_endpoint()
                alarm_stats = np.reshape(ds.occurrence_node, (int(ds.occurrence_node.size / 2), 2))

                for alarm in alarm_stats:
                    print("{0:>8.0f} ".format(round((ds.dt[alarm[1]] - ds.dt[alarm[0]])/60)))
        sleep(0.1)

    def test_dataseries_max_lasting_time(self):
        df = DataImporter.xls_import('SampleData.xlsx')
        for idx, col in enumerate(df.columns):
            if col[0:7] != 'Unnamed':
                ds = DataSeries(df, idx)
                ds.report()
                # print("{}    {}".format(len(ds.alarm_array), len(ds.occurrence)))
        sleep(0.1)


class TestDataImport(unittest.TestCase):

    def test_xls_import(self):
        df = DataImporter.xls_import('SampleData.xlsx')
        for idx, col in enumerate(df.columns):
            if col[0:7] != 'Unnamed':
                ds = DataSeries(df, idx)
                print(ds.analyze(1))
