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

