import pandas as pd
import numpy as np
import yaml
import pathlib
import os


class DataSeries:
    gno = None
    sno = None
    sname = None
    def __init__(self, df: pd.DataFrame, col_idx, device_name=None):
        if device_name is not None:
            self.device_name = device_name
        else:
            self.device_name = df.columns[col_idx]
        self.occurrence = None
        self.occurrence_node = None
        self.alarm_stats = None
        self.alarm_array = None
        self.lasting_minutes = None
        self.max_mv = None
        self.max_lasting = None

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

    def analyze(self, ratio_factor):
        """
        :param ratio_factor: 標準差倍數
        """
        threshold = self.get_voltage_threshold(ratio_factor)
        self.alarm_array = np.where(self.voltage >= threshold, 1, 0)  # 將高於門檻值標示成 1 否則為 0
        self.occurrence = np.diff(self.alarm_array)  # 清理轉換成元素差值 n+1 - n
        self.occurrence_node = self.occurrence.nonzero()[0]  # 取得非零元素的索引形成 tuple
        self.clean_series_endpoint()  # 移除無用開頭或結尾處於 Alarm 狀態 -> 無從估計持續時間
        self.alarm_stats = np.reshape(self.occurrence_node, (int(self.occurrence_node.size / 2), 2))

        # round((ds.dt[alarm[1]].timestamp() - ds.dt[alarm[0]].timestamp()) / 60))
        vf = np.vectorize(lambda x: self.dt[x])
        self.lasting_minutes = vf(self.alarm_stats[:, 1, ]) - vf(self.alarm_stats[:, 0, ])
        return self.alarm_stats

    def get_voltage_threshold(self, ratio_factor):  # 門檻值必須 >= 5
        threshold = self.voltage.mean() + ratio_factor * self.voltage.std()

        if threshold <= 5:
            return 5

        if self.max_mv is not None and self.max_mv != 0 and threshold >= self.max_mv:
            return self.max_mv

        return threshold

    def get_max_lasting_minutes(self):  # 持續時間必須介於 20 ~ self.max_lasting min
        max_lasting_minute = np.amax(self.lasting_minutes)
        if max_lasting_minute <= 20:
            return 20

        if self.max_lasting is not None and self.max_lasting != 0 and max_lasting_minute >= self.max_lasting:
            return self.max_lasting

        return max_lasting_minute

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
                self.report_list[self.device_name] = dict()
                for ratio in self.ratio_list:
                    try:
                        self.analyze(ratio)
                    except IndexError as e:
                        print("Ratio={} - {}".format(ratio, str(e)))
                        continue
                    self.report_list[self.device_name][str(ratio)] = [
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
