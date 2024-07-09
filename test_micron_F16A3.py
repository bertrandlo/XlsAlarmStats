import unittest

from pdstat import evaluating_thresholding


class TestMicronF16A3Analysis(unittest.TestCase):

    def test_evaluating(self):

        from pdcomponent.customer import Customer

        cu = Customer(98)  # 美光F16A3
        target_list = cu.get_all_devices()

        ratio_list = [1, 2, 3]

        dt_begin = "2024-01-01T00:00:00"
        dt_end = "2024-05-31T23:59:59"

        evaluating_mv = 0.5
        evaluating_duration_minutes = [10, 30, 60, 120, 180]
        evaluating_thresholding("Report", target_list, dt_begin, dt_end,
                                evaluating_duration_minutes, evaluating_mv, ratio_list)

