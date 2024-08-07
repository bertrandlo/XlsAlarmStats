import pandas as pd
import numpy as np
import yaml
import pathlib
import os


class DataSeries:
    gno = None
    sno = None
    sname = None
    channel = None

    def __init__(self, df: pd.DataFrame, col_idx, device_name=None, station_name=None):
        if device_name is not None:
            self.device_name = device_name
        else:
            self.device_name = df.columns[col_idx]

        if station_name is not None:
            self.station_name = station_name
        else:
            self.station_name = station_name

        self.occurrence = None
        self.occurrence_node = None
        self.alarm_stats = None
        self.alarm_array = None
        self.lasting_minutes = None
        self.max_mv = None
        self.max_lasting = None

        self.ratio_list: list = []

        self.voltage_threshold_max = 80
        self.voltage_threshold_min = 5
        self.lasting_minutes_max = 1440
        self.lasting_minutes_min = 20

        vf = np.vectorize(lambda x: x.timestamp()/60)
        self.dt = vf(df.iloc[2:, col_idx])
        self.voltage = df.iloc[2:, col_idx + 1].to_numpy()  # type: np.ndarray
        self.pps = df.iloc[2:, col_idx + 2].to_numpy()  # type: np.ndarray

        p = pathlib.Path(os.getcwd()).joinpath("config.yml")
        try:
            with open(p, "r") as stream:
                data: dict
                data = yaml.load(stream)
            self.ratio_list = data['std_ratio']
        except IOError:
            self.ratio_list = [1, 2, 3]
        self.report_list = dict()

    def __str__(self):
        thresholding = []
        for ratio in self.ratio_list:
            thresholding.append("\"{}xSTD\"={:.1f}".format(ratio, self.voltage.mean() + self.voltage.std()*ratio))

        return "{} - CH={} - radios={}, mean={:.1f}, std={:.1f}, {}".format(
            self.device_name, self.channel, self.ratio_list, self.voltage.mean(), self.voltage.std(), ",".join(thresholding))

    def analyze(self, ratio_factor):
        """
        :param ratio_factor: 標準差倍數
        """
        threshold = self.get_voltage_threshold(ratio_factor)
        self.analyze_by_specific_voltage(threshold)
        return self.alarm_stats

    def analyze_by_specific_voltage(self, volt: float):
        self.alarm_array = np.where(self.voltage >= volt, 1, 0)  # 將高於門檻值標示成 1 否則為 0
        self.occurrence = np.diff(self.alarm_array)  # 清理轉換成元素差值 n+1 - n
        self.occurrence_node = self.occurrence.nonzero()[0]  # 取得非零元素的索引形成 tuple
        self.clean_series_endpoint()  # 移除無用開頭或結尾處於 Alarm 狀態 -> 無從估計持續時間
        self.alarm_stats = np.reshape(self.occurrence_node, (int(self.occurrence_node.size / 2), 2))

        # round((ds.dt[alarm[1]].timestamp() - ds.dt[alarm[0]].timestamp()) / 60))
        vf = np.vectorize(lambda x: self.dt[x])
        self.lasting_minutes = vf(self.alarm_stats[:, 1, ]) - vf(self.alarm_stats[:, 0, ])

    def get_voltage_threshold(self, ratio_factor):  # 門檻值預設必須介於 5mV ~ 80mV
        threshold = self.voltage.mean() + ratio_factor * self.voltage.std()
        if threshold <= 5:
            return 5
        return threshold

    def get_max_lasting_minutes(self):  # 持續時間預設必須介於 20 ~ 1440 min
        max_lasting_minute = np.amax(self.lasting_minutes)
        if max_lasting_minute is None or max_lasting_minute <= self.lasting_minutes_min:
            return self.lasting_minutes_min
        if max_lasting_minute >= self.lasting_minutes_max:
            return self.lasting_minutes_max
        return max_lasting_minute

    def count_occurrence(self, duration_min: int):
        from datetime import datetime
        occurrence = 0
        occurrence_info = []
        for event in self.alarm_stats:
            if (event[1] - event[0]) >= duration_min:
                occurrence = occurrence + 1
                occurrence_info.append([datetime.fromtimestamp(self.dt[event[0]]*60), datetime.fromtimestamp(self.dt[event[1]]*60)])

        return occurrence, occurrence_info

    def get_occurrence_node_mapping(self):
        return np.fromfunction(lambda i, j: self.occurrence[self.alarm_stats[i, j]], (int(self.occurrence_node.size / 2), 2), dtype=int)

    def clean_series_endpoint(self):
        try:
            if self.occurrence[self.occurrence_node[0]] == -1:  # 開頭就處於觸發狀態
                self.occurrence_node = np.insert(self.occurrence_node, 0, 0)  # 標示數列開頭是節點
            if self.occurrence[self.occurrence_node[-1]] == 1:
                self.occurrence_node = np.append(self.occurrence_node, len(self.occurrence)-1)  # 標示數列尾端是節點
        except IndexError:
            raise IndexError("無法計算時間閾值！")

    def report(self):
                print("\ngroup no: {}  - group name: {}, station no: {}, station name: {}".format(
                    self.gno,
                    self.device_name,
                    self.sno if self.sno is not None else " ",
                    self.sname if self.sname is not None else " ",
                ))
                self.report_list = dict()
                for ratio in self.ratio_list:
                    try:
                        self.analyze(ratio)
                    except IndexError as e:
                        print("Ratio={} - {}".format(ratio, str(e)))
                        continue
                    self.report_list[str(ratio)] = [
                        "{0:.1f}".format(self.voltage.mean()),
                        "{0:.1f}".format(self.voltage.std()),
                        "{0:.1f}".format(self.get_voltage_threshold(ratio)),
                        "{0:.1f}".format(self.get_max_lasting_minutes()),
                        str(np.sum(np.where(self.lasting_minutes >= self.get_max_lasting_minutes(), 1, 0), dtype=np.int32))]

                    print("mean: {0:>6.1f}, std: {1:>6.2f}, threshold: {2:>6.2f}, Tmax: {3:>6.0f}, Alarm #: {4:>4}".format(
                        self.voltage.mean(),
                        self.voltage.std(),
                        self.get_voltage_threshold(ratio),
                        self.get_max_lasting_minutes(),
                        np.sum(np.where(self.lasting_minutes >= self.get_max_lasting_minutes(), 1, 0), dtype=np.int32)))


class DataImporter:

    @classmethod
    def xls_import(cls, file_name):
        data = pd.read_excel(file_name)
        return data
