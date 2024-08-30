import datetime
import pickle
from pathlib import Path

import numpy
import numpy as np
import json
import unittest

from matplotlib import pyplot as plt
from matplotlib.ticker import FormatStrFormatter

from customer_grouping import load_csv
from pdstat import evaluating_thresholding
import pandas as pd


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

    def test_hello_world(self):
        breakpoint()
        print("HELLO")

    def test_discharge_evaluating(self):
        from pdcomponent.device import Device

        """
            12AP3P4	MTR301	有監控	12870	12871	12872
            12AP3P4	MTR302	有監控	12876	12877	12878
            12AP3P4	MTR303	有監控	12879	12880	12881
            12AP3P4	MTR304	有監控	12873	12874	12875
            12AP3P4	UTR301	有監控	12961	12962	12963
            12AP3P4	UTR302	有監控	12966	12967	12968
            12AP3P4	UTR303	有監控	13001	13002	13003
            12AP3P4	UTR304	有監控	13006	13007	13008
            12AP3P4	UTR305	有監控	12971	12972	12973
            12AP3P4	UTR306	有監控	12976	12977	12978
            12AP3P4	UTR307	有監控	12981	12982	12983
            12AP3P4	UTR308	有監控	12986	12987	12988
            12AP3P4	UTR309	有監控	12991	12992	12993
            12AP3P4	UTR310	有監控	12996	12997	12998
            12AP3P4	UTR311	有監控	13011	13012	13013

            12AP5P6	161kV_TR03      有監控(主變)	15812
            12AP5P6	161kV_TR04	    有監控(主變)	15814
            12AP5P6	MTRE103	        有監控	    6694	

            8AB	BTR211	有監控	11543
            8AB	MTR108	有監控	11454
            8AB	OTR104	有監控	11552
            8AB	OTR204	有監控	11555

            8CD	MTR05	有監控	10771
            8CD	MTR06	有監控	10772
            8CD	MTR07	有監控	10773
            8CD	STR04	有監控	10194
            8CD	STR06	有監控	11307
            8CD	STR09	有監控	11310
            8CD	CTR07	有監控	11262
            8CD	DTR02	有監控	10184
            8CD	DTR03	有監控	10185
            8CD	DTR12	有監控	11289
            8CD	OTR02	有監控	10189
            8CD	OTR03	有監控	11292

        """
        target_list = [
            # 12AP3P4
            Device(12870), Device(12871), Device(12872),
            Device(12876), Device(12877), Device(12878),
            Device(12879), Device(12880), Device(12881),
            Device(12873), Device(12874), Device(12875),
            Device(12961), Device(12962), Device(12963),
            Device(12966), Device(12967), Device(12968),
            Device(13001), Device(13002), Device(13003),
            Device(13006), Device(13007), Device(13008),
            Device(12971), Device(12972), Device(12973),
            Device(12976), Device(12977), Device(12978),
            Device(12981), Device(12982), Device(12983),
            Device(12986), Device(12987), Device(12988),
            Device(12991), Device(12992), Device(12993),
            Device(12996), Device(12997), Device(12998),
            Device(13011), Device(13012), Device(13013),
            # 12AP5P6
            Device(15812), Device(15814), Device(6694),
            # 8AB
            Device(11543), Device(11454), Device(11552), Device(11555),
            # 8CD
            Device(10771), Device(10772), Device(10773), Device(10194), Device(11307), Device(11310),
            Device(11262), Device(10184), Device(10185), Device(11289), Device(10189), Device(11292)
        ]

        non_seec_target_list = [
            # UMC 12AP1P2
            Device(7060), Device(7061), Device(7062), Device(7072), Device(7090),
            Device(7135), Device(7136), Device(7137), Device(7138), Device(7139),
            Device(10028), Device(10030), Device(10032), Device(10034), Device(10058),

            # UMC 12AP3P4
            Device(6945), Device(6946), Device(6947), Device(6948), Device(6949),
            Device(6950), Device(6951), Device(6952), Device(6953), Device(6954),
            Device(6955), Device(6956), Device(12882), Device(12883), Device(12884),
            Device(12885), Device(12886), Device(12887), Device(12888), Device(12889),
            Device(12890), Device(12891), Device(12892), Device(12893), Device(12894),
            Device(12895), Device(12896), Device(12897), Device(12898), Device(12899),
            Device(12900), Device(12901), Device(12902), Device(12903), Device(12904),
            Device(12905),

            # UMC 12AP5
            Device(6688), Device(6689), Device(6690), Device(6691), Device(6692),
            Device(6693), Device(6694), Device(6695), Device(15369), Device(15370),
            Device(15826), Device(15827), Device(15828), Device(15829), Device(15830),
            Device(15831), Device(15832), Device(15833), Device(15834), Device(15835),

            # UMC 8AB
            Device(6868), Device(6869), Device(6870), Device(6871), Device(6872),
            Device(11452), Device(11453), Device(11454), Device(16417), Device(16592),
            Device(17352), Device(17353),

            # UMC 8CD
            Device(10767), Device(10768), Device(10769), Device(10770), Device(10771),
            Device(10772), Device(10773), Device(10774), Device(10793), Device(10795),
            Device(10797), Device(10799), Device(10801), Device(10803), Device(15439),
            Device(15440), Device(15441),

            # UMC 8E
            Device(12400), Device(12401), Device(12408), Device(12409),

            # UMC 8F
            Device(10205), Device(10206), Device(10207), Device(10208), Device(10209),
            Device(10235), Device(12232),

            # UMC 8S
            Device(12364), Device(12365), Device(12366), Device(12367),

            # UMC SG
            Device(7499), Device(7500), Device(7513), Device(7514), Device(7533),
            Device(7540), Device(7550), Device(7551), Device(7552), Device(7602),
            Device(7619), Device(7620), Device(7621), Device(7622), Device(7623),
            Device(7797), Device(7798), Device(14639), Device(14642), Device(14656),
            Device(14657), Device(14658)
        ]

        ratio_list = [0.5, 1, 2]

        dt_begin = "2023-06-01T00:00:00"
        dt_end = "2024-05-31T23:59:59"

        evaluating_mv = 5
        evaluating_duration_minutes = [10]
        evaluating_thresholding("Report", recollect, dt_begin, dt_end,
                                evaluating_duration_minutes, evaluating_mv, ratio_list)


    # 非士林電機 MTR 清單
    def test_discharge_evaluating_for_non_seec(self):
        from pdcomponent.device import Device

        non_seec_target_list = [
            # UMC 12AP1P2
            Device(7060), Device(7061), Device(7062), Device(7072), Device(7090),
            Device(7135), Device(7136), Device(7137), Device(7138), Device(7139),
            Device(10028), Device(10030), Device(10032), Device(10034), Device(10058),

            # UMC 12AP3P4
            Device(6945), Device(6946), Device(6947), Device(6948), Device(6949),
            Device(6950), Device(6951), Device(6952), Device(6953), Device(6954),
            Device(6955), Device(6956), Device(12882), Device(12883), Device(12884),
            Device(12885), Device(12886), Device(12887), Device(12888), Device(12889),
            Device(12890), Device(12891), Device(12892), Device(12893), Device(12894),
            Device(12895), Device(12896), Device(12897), Device(12898), Device(12899),
            Device(12900), Device(12901), Device(12902), Device(12903), Device(12904),
            Device(12905),

            # UMC 12AP5
            Device(6688), Device(6689), Device(6690), Device(6691), Device(6692),
            Device(6693), Device(6694), Device(6695), Device(15369), Device(15370),
            Device(15826), Device(15827), Device(15828), Device(15829), Device(15830),
            Device(15831), Device(15832), Device(15833), Device(15834), Device(15835),

            # UMC 8AB
            Device(6868), Device(6869), Device(6870), Device(6871), Device(6872),
            Device(11452), Device(11453), Device(11454), Device(16417), Device(16592),
            Device(17352), Device(17353),

            # UMC 8CD
            Device(10767), Device(10768), Device(10769), Device(10770), Device(10771),
            Device(10772), Device(10773), Device(10774), Device(10793), Device(10795),
            Device(10797), Device(10799), Device(10801), Device(10803), Device(15439),
            Device(15440), Device(15441),

            # UMC 8E
            Device(12400), Device(12401), Device(12408), Device(12409),

            # UMC 8F
            Device(10205), Device(10206), Device(10207), Device(10208), Device(10209),
            Device(10235), Device(12232),

            # UMC 8S
            Device(12364), Device(12365), Device(12366), Device(12367),

            # UMC SG
            Device(7499), Device(7500), Device(7513), Device(7514), Device(7533),
            Device(7540), Device(7550), Device(7551), Device(7552), Device(7602),
            Device(7619), Device(7620), Device(7621), Device(7622), Device(7623),
            Device(7797), Device(7798), Device(14639), Device(14642), Device(14656),
            Device(14657), Device(14658)
        ]

        ratio_list = [0.5, 1, 2]

        dt_begin = "2024-01-01T00:00:00"
        dt_end = "2024-05-31T23:59:59"

        evaluating_mv = 5

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
                result = [response.json()['note']['PRPDAnalysis'], response.json()['note']['trendAnalysis']]
            except json.decoder.JSONDecodeError:
                print(row[0], int(row[1]), int(row[2]), int(row[3]), int(row[4]), "Not Enough Data!")
                continue
            print(row[0], int(row[1]), int(row[2]), int(row[3]), int(row[4]), result)

    def test_evaluating_all_umc_average(self):
        import openpyxl
        import matplotlib.pyplot as plt
        import csv
        from pathlib import Path
        import statistics

        base_path = "G:\\我的雲端硬碟\\10-temp\\data"
        xlsx_list = ["Report_Cu56_UMC_8E", "Report_Cu55_UMC_8S", "Report_Cu46_UMC_8F", "Report_Cu45_UMC_8CD",
                     "Report_Cu35_USC_12x", "Report_Cu35_UMC_SG", "Report_Cu35_UMC_12AP3P4",
                     "Report_Cu35_UMC_12AP1P2", "Report_Cu35_UMC_8AB", "Report_Cu19_UMC_12AP5"]
        total_row_count = 0

        mean_distribution = []
        std_distribution = []
        result = []

        for xlsx_name in xlsx_list:
            workbook = openpyxl.load_workbook(Path(base_path) / Path(xlsx_name + ".xlsx"))
            worksheet = workbook['Report']
            gno = None
            for idx, row in enumerate(worksheet.rows):
                try:
                    if row[0].value is not None:
                        gno = int(str(row[0].value).split("_")[0])
                        continue

                    if (row[1].value in [1, 2, 3, 4, 5, 6] and row[2].value is not None and
                            (float(str(row[2].value)) and float(str(row[3].value)))):
                        # print([row[2].value, row[3].value])
                        total_row_count = total_row_count + 1
                        mean_distribution.append(row[2].value)
                        std_distribution.append(row[3].value)
                        result.append([gno, row[2].value, row[3].value])
                except ValueError:
                    print(xlsx_name, idx, [cell.value for cell in row])
                    continue

        print("total row counts = {}".format(total_row_count))
        print("average mv = {}, average std = {}".format(statistics.mean(mean_distribution), statistics.mean(std_distribution)))

        print("exceed >= 30mV, #={}".format([(elem >= 30.0) for elem in mean_distribution].count(True)))
        print("exceed >= 25mV, #={}".format([(elem >= 25.0) for elem in mean_distribution].count(True)))
        print("exceed >= 20mV, #={}".format([(elem >= 20.0) for elem in mean_distribution].count(True)))
        print("exceed >= 15mV, #={}".format([(elem >= 15.0) for elem in mean_distribution].count(True)))
        print("exceed >= 10mV, #={}".format([(elem >= 10.0) for elem in mean_distribution].count(True)))

        fig, ax = plt.subplots(figsize=(4, 3))
        sc = ax.scatter(mean_distribution, std_distribution, marker="o", s=12, edgecolors="none", alpha=0.25)
        sc.set_facecolor("red")
        ax.set_xlabel("mean")
        ax.set_ylabel("std")

        plt.xlim(0, 20)
        plt.ylim(0, 10)
        plt.show()

        with open("result.csv", "w", newline="\n") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(result)

    def test_evaluating_by_device_type(self):
        import openpyxl
        from pathlib import Path
        import matplotlib.pyplot as plt
        import csv
        import statistics

        db = dict()
        with open("result.csv", "r", newline="\n") as csv_file:
            reader = csv.reader(csv_file)
            data = [[int(row[0]), float(row[1]), float(row[2])] for row in reader if row]

        for r in data:
            db[int(r[0])] = r

        base_path = "G:\\我的雲端硬碟\\10-temp\\data"
        xlsx_file = "閾值分類估算.xlsx"
        sheets = ["CDA", "MTR", "GIS"]

        workbook = openpyxl.load_workbook(Path(base_path) / Path(xlsx_file))
        for sheet in sheets:
            worksheet = workbook[sheet]
            mean_distribution = []
            std_distribution = []
            for idx, row in enumerate(list(worksheet.rows)[1:]):
                try:
                    mean_distribution.append(db[row[4].value][1])
                    std_distribution.append(db[row[4].value][2])
                    # print(sheet, row[4].value, db[row[4].value], [cell.value for cell in row])
                except KeyError:
                    continue
            print("For {}, count = {}, average mv = {}, average std = {}".format(
                sheet, len(mean_distribution), statistics.mean(mean_distribution), statistics.mean(std_distribution)))

            fig, ax = plt.subplots(figsize=(4, 3))
            ax.set_title(sheet)
            sc = ax.scatter(mean_distribution, std_distribution, marker="o", s=12, edgecolors="none", alpha=0.25)
            sc.set_facecolor("red")
            ax.set_xlabel("mean")
            ax.set_ylabel("std")

            #plt.xlim(0, 20)
            #plt.ylim(0, 10)
            plt.show()

    def test_visualizing(self):

        LINE_COLOR = ["#FF1A1A", "#EC706380", "#00808080", "#AF7AC580", "#9FE2BF80", "#45454580", "#BFBDBD",
                      "#E6EA4680", "#666633"]

        data_files = {
            '5mV, 10min': Path('occurrence_info_5mv_10min.csv'),
            '12mV, 10min': Path('occurrence_info_12mv_10min.csv')
        }

        header, csv_rows_5mv_10min = load_csv(csv_file=data_files['5mV, 10min'])
        header, csv_rows_12mv_10min = load_csv(csv_file=data_files['12mV, 10min'])

        group = [{
            'UMC 12AP3P4__CUB_3F__MTR301': [12870, 12871, 12872],
            'UMC 12AP3P4__CUB_3F__MTR302': [12876, 12877, 12878],
            'UMC 12AP3P4__CUB_3F__MTR303': [12879, 12880, 12881],
            'UMC 12AP3P4__CUB_3F__MTR304': [12873, 12874, 12875],
            'UMC 12AP3P4__UB_4F-S__UTR301': [12961, 12962, 12963],
            'UMC 12AP3P4__UB_4F-S__UTR302': [12966, 12967, 12968],
            'UMC 12AP3P4__UB_4F-N__UTR303': [13001, 13002, 13003],
            'UMC 12AP3P4__UB_4F-N__UTR304': [13006, 13007, 13008],
            'UMC 12AP3P4__UB_4F-S__UTR305': [12971, 12972, 12973],
            'UMC 12AP3P4__UB_4F-S__UTR306': [12976, 12977, 12978],
            'UMC 12AP3P4__UB_4F-N__UTR307': [12981, 12982, 12983],
            'UMC 12AP3P4__UB_4F-N__UTR308': [12986, 12987, 12988],
            'UMC 12AP3P4__UB_4F-N__UTR309': [12991, 12992, 12993],
            'UMC 12AP3P4__UB_4F-N__UTR310': [12996, 12997, 12998],
            'UMC 12AP3P4__UB_4F-N__UTR311': [13011, 13012, 13013]

        }, {
            'UMC 12AP5P6__CUB_P5_3F__MTRE103': [6694],
            'UMC 12AP5P6__PSB_P5_1F__161kV_TR03': [15812],
            'UMC 12AP5P6__PSB_P5_1F__161kV_TR04': [15814],
        }, {
            'UMC 8AB__SBF電氣室__BTR211': [11543],
            'UMC 8AB__M1F__MTR108': [11454],
            'UMC 8AB__O5F電氣室__OTR104': [11552],
            'UMC 8AB__O5F電氣室__OTR204': [11555],
            'UMC 8AB__SBF電氣室__S701': [11516]
        }, {
            'UMC 8CD__CUB變電站__MTR-05': [10771],
            'UMC 8CD__CUB變電站__MTR-06': [10772],
            'UMC 8CD__CUB變電站__MTR-07': [10773],
            'UMC 8CD__SB_Station__STR-04': [10194],
            'UMC 8CD__SB_Station__STR-06': [11307],
            'UMC 8CD__SB_Station__STR-09': [11310],
            'UMC 8CD__BC_Station__CTR-07': [11262],
            'UMC 8CD__BD_Station__DTR-02': [10184],
            'UMC 8CD__BD_Station__DTR-03': [10185],
            'UMC 8CD__BD_Station__DTR-12': [11289],
            'UMC 8CD__UT_Station__OTR-02': [10189],
            'UMC 8CD__UT_Station__OTR-03': [11292],
        }]
        markers = ['o', 'v', '^']
        font_family = {'fontname': 'Noto Sans TC'}

        for cu_elem_idx, cu in enumerate(csv_rows_12mv_10min):

            for tag, gno_list in group[cu_elem_idx].items():
                if 11516 in gno_list:
                    print("HELLO")
                fig, ax = plt.subplots(figsize=(12, 6))
                plt.title(tag, **font_family)
                ax.yaxis.set_major_formatter(FormatStrFormatter('%.0f'))

                print(tag, gno_list)
                max_count = 10

                # 移除 tag, min, std
                raw_dataset_12mv_10min = []
                for elem in cu:
                    if elem[0] in gno_list:
                        raw_dataset_12mv_10min.append(elem)

                dataset_12mv_10min = np.array(np.delete(np.array(raw_dataset_12mv_10min), (0, 1, 2, 3, 4), 1), dtype=np.int64)
                channels = np.array(raw_dataset_12mv_10min)[:, 1].tolist()
                print('12mv, 10min', dataset_12mv_10min)

                line_12mv_10min = []
                for row_idx in range(dataset_12mv_10min.shape[0]):
                    _line_, = plt.plot(header[1:], dataset_12mv_10min[row_idx].tolist(),
                                       linestyle='solid', color=LINE_COLOR[row_idx], marker=markers[0], alpha=0.5)
                    line_12mv_10min.append(_line_)

                ax.annotate('mean={}, std={}'.format(np.array(raw_dataset_12mv_10min)[:, 3].tolist(),
                                                     np.array(raw_dataset_12mv_10min)[:, 4].tolist()),
                            xycoords='figure fraction', xy=(0.15, 0.9), **font_family)

                raw_dataset_5mv_10min = []
                for elem in csv_rows_5mv_10min[cu_elem_idx]:
                    if elem[0] in gno_list:
                        raw_dataset_5mv_10min.append(elem)
                dataset_5mv_10min = np.array(np.delete(np.array(raw_dataset_5mv_10min), (0, 1, 2, 3, 4), 1), dtype=np.int64)
                line_5mv_10min = []
                for row_idx in range(dataset_12mv_10min.shape[0]):
                    _line_, = plt.plot(header[1:], dataset_5mv_10min[row_idx].tolist(),
                                       linestyle='dashed', color=LINE_COLOR[row_idx], marker=markers[1], alpha=0.5)
                    line_5mv_10min.append(_line_)

                ax.set_ylim(0, max_count)
                print('5mv, 10min', dataset_5mv_10min)
                ymax = max(dataset_5mv_10min.max(axis=0).max(), dataset_12mv_10min.max(axis=0).max())

                if ymax > max_count:
                    ax.set_ylim(0, ((ymax//10)+1)*10)

                ax.legend([line for line in line_5mv_10min] + [line for line in line_12mv_10min],
                          ['5mV, 10min, CH{}'.format(channel) for channel in channels] +
                          ['12mV, 10min, CH{}'.format(channel) for channel in channels],
                          loc='upper left', shadow=False, borderpad=0, frameon=False)
                plt.savefig('{}_{}.png'.format(tag, '_'.join([str(gno) for gno in gno_list])))
                print("-----------------------------------------------------------------------------------")

    def test_load_event(self):
        from pdcomponent.core import loadNotificationAlarmEvent

        groups = [{
            'UMC 12AP3P4__CUB_3F__MTR301': [12870, 12871, 12872],
            'UMC 12AP3P4__CUB_3F__MTR302': [12876, 12877, 12878],
            'UMC 12AP3P4__CUB_3F__MTR303': [12879, 12880, 12881],
            'UMC 12AP3P4__CUB_3F__MTR304': [12873, 12874, 12875],
            'UMC 12AP3P4__UB_4F-S__UTR301': [12961, 12962, 12963],
            'UMC 12AP3P4__UB_4F-S__UTR302': [12966, 12967, 12968],
            'UMC 12AP3P4__UB_4F-N__UTR303': [13001, 13002, 13003],
            'UMC 12AP3P4__UB_4F-N__UTR304': [13006, 13007, 13008],
            'UMC 12AP3P4__UB_4F-S__UTR305': [12971, 12972, 12973],
            'UMC 12AP3P4__UB_4F-S__UTR306': [12976, 12977, 12978],
            'UMC 12AP3P4__UB_4F-N__UTR307': [12981, 12982, 12983],
            'UMC 12AP3P4__UB_4F-N__UTR308': [12986, 12987, 12988],
            'UMC 12AP3P4__UB_4F-N__UTR309': [12991, 12992, 12993],
            'UMC 12AP3P4__UB_4F-N__UTR310': [12996, 12997, 12998],
            'UMC 12AP3P4__UB_4F-N__UTR311': [13011, 13012, 13013]

        }, {
            'UMC 12AP5P6__CUB_P5_3F__MTRE103': [6694],
            'UMC 12AP5P6__PSB_P5_1F__161kV_TR03': [15812],
            'UMC 12AP5P6__PSB_P5_1F__161kV_TR04': [15814],
        }, {
            'UMC 8AB__SBF電氣室__BTR211': [11543],
            'UMC 8AB__M1F__MTR108': [11454],
            'UMC 8AB__O5F電氣室__OTR104': [11552],
            'UMC 8AB__O5F電氣室__OTR204': [11555],
            'UMC 8AB__SBF電氣室__S701': [11516]
        }, {
            'UMC 8CD__CUB變電站__MTR-05': [10771],
            'UMC 8CD__CUB變電站__MTR-06': [10772],
            'UMC 8CD__CUB變電站__MTR-07': [10773],
            'UMC 8CD__SB_Station__STR-04': [10194],
            'UMC 8CD__SB_Station__STR-06': [11307],
            'UMC 8CD__SB_Station__STR-09': [11310],
            'UMC 8CD__BC_Station__CTR-07': [11262],
            'UMC 8CD__BD_Station__DTR-02': [10184],
            'UMC 8CD__BD_Station__DTR-03': [10185],
            'UMC 8CD__BD_Station__DTR-12': [11289],
            'UMC 8CD__UT_Station__OTR-02': [10189],
            'UMC 8CD__UT_Station__OTR-03': [11292],
        }]

        non_seec_target_list = [
            # UMC 12AP1P2
            7060, 7061, 7062, 7072, 7090,
            7135, 7136, 7137, 7138, 7139,
            10028, 10030, 10032, 10034, 10058,

            # UMC 12AP3P4
            6945, 6946, 6947, 6948, 6949,
            6950, 6951, 6952, 6953, 6954,
            6955, 6956, 12882, 12883, 12884,
            12885, 12886, 12887, 12888, 12889,
            12890, 12891, 12892, 12893, 12894,
            12895, 12896, 12897, 12898, 12899,
            12900, 12901, 12902, 12903, 12904,
            12905,

            # UMC 12AP5
            6688, 6689, 6690, 6691, 6692,
            6693, 6694, 6695, 15369, 15370,
            15826, 15827, 15828, 15829, 15830,
            15831, 15832, 15833, 15834, 15835,

            # UMC 8AB
            6868, 6869, 6870, 6871, 6872,
            11452, 11453, 11454, 16417, 16592,
            17352, 17353,

            # UMC 8CD
            10767, 10768, 10769, 10770, 10771,
            10772, 10773, 10774, 10793, 10795,
            10797, 10799, 10801, 10803, 15439,
            15440, 15441,

            # UMC 8E
            12400, 12401, 12408, 12409,

            # UMC 8F
            10205, 10206, 10207, 10208, 10209,
            10235, 12232,

            # UMC 8S
            12364, 12365, 12366, 12367,

            # UMC SG
            7499, 7500, 7513, 7514, 7533,
            7540, 7550, 7551, 7552, 7602,
            7619, 7620, 7621, 7622, 7623,
            7797, 7798, 14639, 14642, 14656,
            14657, 14658
        ]


        flat_groups = []
        for cu in groups:
            flat_groups = flat_groups + sum(cu.values(), [])

        flat_groups = flat_groups + non_seec_target_list

        print(flat_groups)

        event_collection = loadNotificationAlarmEvent(beginning_date=datetime.datetime(2023, 6, 1, 0, 0, 0),
                                                      ending_date=datetime.datetime(2024, 5, 31, 23, 59, 59))
        alarm_list = []

        for gno, device in event_collection.items():
            if int(gno) in flat_groups:
                alarm_list.append(device)

        with open("alarm_list.pickle", 'wb') as f:
            pickle.dump(alarm_list, f)

    def test_grouping_event(self):
        from pdcomponent.device import Device
        from pdcomponent.core import MysqlPdcNotificationAlarm
        seec_target_list = [
            # 12AP3P4
            12870, 12871, 12872,
            12876, 12877, 12878,
            12879, 12880, 12881,
            12873, 12874, 12875,
            12961, 12962, 12963,
            12966, 12967, 12968,
            13001, 13002, 13003,
            13006, 13007, 13008,
            12971, 12972, 12973,
            12976, 12977, 12978,
            12981, 12982, 12983,
            12986, 12987, 12988,
            12991, 12992, 12993,
            12996, 12997, 12998,
            13011, 13012, 13013,
            # 12AP5P6
            15812, 15814, 6694,
            # 8AB
            11543, 11454, 11552, 11555,
            # 8CD
            10771, 10772, 10773, 10194, 11307, 11310,
            11262, 10184, 10185, 11289, 10189, 11292
        ]

        non_seec_target_list = [
            # UMC 12AP1P2
            7060, 7061, 7062, 7072, 7090,
            7135, 7136, 7137, 7138, 7139,
            10028, 10030, 10032, 10034, 10058,

            # UMC 12AP3P4
            6945, 6946, 6947, 6948, 6949,
            6950, 6951, 6952, 6953, 6954,
            6955, 6956, 12882, 12883, 12884,
            12885, 12886, 12887, 12888, 12889,
            12890, 12891, 12892, 12893, 12894,
            12895, 12896, 12897, 12898, 12899,
            12900, 12901, 12902, 12903, 12904,
            12905,

            # UMC 12AP5
            6688, 6689, 6690, 6691, 6692,
            6693, 6694, 6695, 15369, 15370,
            15826, 15827, 15828, 15829, 15830,
            15831, 15832, 15833, 15834, 15835,

            # UMC 8AB
            6868, 6869, 6870, 6871, 6872,
            11452, 11453, 11454, 16417, 16592,
            17352, 17353,

            # UMC 8CD
            10767, 10768, 10769, 10770, 10771,
            10772, 10773, 10774, 10793, 10795,
            10797, 10799, 10801, 10803, 15439,
            15440, 15441,

            # UMC 8E
            12400, 12401, 12408, 12409,

            # UMC 8F
            10205, 10206, 10207, 10208, 10209,
            10235, 12232,

            # UMC 8S
            12364, 12365, 12366, 12367,

            # UMC SG
            7499, 7500, 7513, 7514, 7533,
            7540, 7550, 7551, 7552, 7602,
            7619, 7620, 7621, 7622, 7623,
            7797, 7798, 14639, 14642, 14656,
            14657, 14658
        ]

        data_files = {
            '5mV, 10min': Path('occurrence_info_5mv_10min.csv'),
            '12mV, 10min': Path('occurrence_info_12mv_10min.csv'),
            '12mV, 120min': Path('occurrence_info_12mv_120min.csv')
        }

        header, csv_rows_12mv_10min = load_csv(csv_file=data_files['12mV, 10min'])
        result = None
        for cu in csv_rows_12mv_10min:
            for ch in cu:
                if result is None:
                    result = np.zeros(12, dtype=np.int64)
                result = np.add(result, np.pad(np.array(ch[5:]), ((0, 12-len(ch[5:])))))

        print(result)
        result = np.zeros(12, dtype=np.int64).tolist()

        with open("alarm_list.pickle", 'rb') as f:
            l = pickle.load(f)
            dev: Device
            for dev in l:
                for risk_level, events in dev.event_group_by_risk.items():
                    if dev.gNo not in seec_target_list and risk_level != 0 and len(events) != 0:
                        #print(dev.gNo, dev.cuName, dev.sName, dev.gName, risk_level, 'count={}'.format(len(events)), events)
                        alarm_map = dev.notificationalarm_collection_dict
                        for nano in dev.event_group_by_risk[risk_level]:
                            idx = month_slot(alarm_map, nano)
                            result[idx] = result[idx] + 1

        print(result)

def month_slot(alarm_map, nano):
    return ((alarm_map[str(nano)].naTime.year - 2023)*12 + (alarm_map[str(nano)].naTime.month - 6))


class TestUMCSEECList(unittest.TestCase):
    def test_load_csv(self):
        from pdcomponent.device import Device
        non_seec_target_list = [
            # UMC 12AP1P2
            Device(7060), Device(7061), Device(7062), Device(7072), Device(7090),
            Device(7135), Device(7136), Device(7137), Device(7138), Device(7139),
            Device(10028), Device(10030), Device(10032), Device(10034), Device(10058),

            # UMC 12AP3P4
            Device(6945), Device(6946), Device(6947), Device(6948), Device(6949),
            Device(6950), Device(6951), Device(6952), Device(6953), Device(6954),
            Device(6955), Device(6956), Device(12882), Device(12883), Device(12884),
            Device(12885), Device(12886), Device(12887), Device(12888), Device(12889),
            Device(12890), Device(12891), Device(12892), Device(12893), Device(12894),
            Device(12895), Device(12896), Device(12897), Device(12898), Device(12899),
            Device(12900), Device(12901), Device(12902), Device(12903), Device(12904),
            Device(12905),

            # UMC 12AP5
            Device(6688), Device(6689), Device(6690), Device(6691), Device(6692),
            Device(6693), Device(6694), Device(6695), Device(15369), Device(15370),
            Device(15826), Device(15827), Device(15828), Device(15829), Device(15830),
            Device(15831), Device(15832), Device(15833), Device(15834), Device(15835),

            # UMC 8AB
            Device(6868), Device(6869), Device(6870), Device(6871), Device(6872),
            Device(11452), Device(11453), Device(11454), Device(16417), Device(16592),
            Device(17352), Device(17353),

            # UMC 8CD
            Device(10767), Device(10768), Device(10769), Device(10770), Device(10771),
            Device(10772), Device(10773), Device(10774), Device(10793), Device(10795),
            Device(10797), Device(10799), Device(10801), Device(10803), Device(15439),
            Device(15440), Device(15441),

            # UMC 8E
            Device(12400), Device(12401), Device(12408), Device(12409),

            # UMC 8F
            Device(10205), Device(10206), Device(10207), Device(10208), Device(10209),
            Device(10235), Device(12232),

            # UMC 8S
            Device(12364), Device(12365), Device(12366), Device(12367),

            # UMC SG
            Device(7499), Device(7500), Device(7513), Device(7514), Device(7533),
            Device(7540), Device(7550), Device(7551), Device(7552), Device(7602),
            Device(7619), Device(7620), Device(7621), Device(7622), Device(7623),
            Device(7797), Device(7798), Device(14639), Device(14642), Device(14656),
            Device(14657), Device(14658)
        ]

        for dev in non_seec_target_list:
            print(dev.cuName, dev.sName, dev.gName, dev.get_channels_list(), dev.gNo)

