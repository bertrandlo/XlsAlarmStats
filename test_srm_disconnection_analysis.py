import unittest
from pathlib import Path
import sqlite3
import numpy as np
import pandas as pd

class MyTestCase(unittest.TestCase):
    def test_something(self):
        # 2025-11-06
        begin_datetime = '2025-10-24 00:00:00+08:00'
        end_datetime = '2025-11-07 00:00:00+08:00'
        begin_ts = int(pd.to_datetime(begin_datetime).timestamp())
        end_ts = int(pd.to_datetime(end_datetime).timestamp())

        dataset_base_path = Path('.\\data\\')
        sqlite_db_files = ['00000002-0001-0001-0003-000000000001_202510.db',
                           '00000002-0001-0001-0003-000000000001_202511.db']

        _matrix = []
        # PDTrend PDPattern
        for f in sqlite_db_files:
            conn = sqlite3.connect(dataset_base_path / f)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT recordDate FROM PDTrend WHERE recordDate > {} AND recordDate < {} ORDER BY recordDate ASC"
                           .format(begin_ts, end_ts))
            _rows = cursor.fetchall()
            _matrix.append(np.array(_rows))


        dataset = None
        for elem in _matrix:
            if dataset is None:
                dataset = elem
            else:
                dataset = np.vstack((dataset, elem))

        col_record_date = dataset[:, 0]
        col_record_date_diff = np.diff(col_record_date)
        diff_matrix = col_record_date_diff.reshape(-1, 1)
        diff_padded = np.append(diff_matrix, np.nan)
        # 組成新的矩陣：原第二欄 + 差值欄
        new_matrix = np.column_stack((col_record_date, diff_padded))

        df = pd.DataFrame(new_matrix, columns=['dt', 'distance'])
        df_sorted = df.sort_values(by='distance', ascending=False)
        df_sorted['datetime_local'] = pd.to_datetime(df_sorted['dt'], unit='s', utc=True).dt.tz_convert('Asia/Taipei')

        print(df_sorted[:10])


if __name__ == '__main__':
    unittest.main()
