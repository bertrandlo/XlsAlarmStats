import unittest

from pdstat import evaluating_thresholding


class TestUMC8SAnalysis(unittest.TestCase):

    def test_evaluating(self):

        from pdcomponent.customer import Customer

        cu = Customer(55)
        target_list = cu.get_all_devices()[:5]

        ratio_list = [0.5, 1, 2]

        dt_begin = "2024-01-01T00:00:00"
        dt_end = "2024-05-31T23:59:59"

        evaluating_mv = 5
        evaluating_duration_minutes = [10, 15, 20, 25, 30]
        evaluating_thresholding("Report", target_list, dt_begin, dt_end,
                                evaluating_duration_minutes, evaluating_mv, ratio_list)

    def test_discharge_evaluating(self):
        from pdcomponent.device import Device

        target_list = [Device(12348)]  # UMC-8S  HV-12M

        ratio_list = [0.5, 1, 2]

        dt_begin = "2020-01-01T00:00:00"
        dt_end = "2020-05-31T23:59:59"

        evaluating_mv = 0.5

        evaluating_duration_minutes = [10, 15, 20, 25, 30]
        evaluating_thresholding("Report", target_list, dt_begin, dt_end,
                                evaluating_duration_minutes, evaluating_mv, ratio_list)

    def test_TSMC_AIService_validation_golden_set(self):
        import requests
        import pandas

        api_url = "http://archive.pdcare.com:9998/getAnalysisByEvent"
        df = pandas.read_csv('golden_samples_event.csv', sep=",", encoding="utf-8", header=None)
        headers = {"Content-Type": "application/json"}
        for idx, row in df.iterrows():
            response = requests.post(api_url, headers=headers, data=json.dumps({"alarmID": int(row[1])}))
            try:
                result = [response.json()['note']['PRPDAnalysis'],response.json()['note']['trendAnalysis']]
            except json.decoder.JSONDecodeError:
                print(row[0], int(row[1]), int(row[2]), int(row[3]), int(row[4]), "Not Enough Data!")
                continue
            print(row[0], int(row[1]), int(row[2]), int(row[3]), int(row[4]), result)
