import csv
import datetime
import logging
import math
import pickle
import unittest
from datetime import timezone, timedelta
from pathlib import Path


from customer_grouping import Triggering, CustomerGrouping
from pdcomponent import session
from pdcomponent.core import MysqlCustomer, MysqlOverviewEntity, full2half
from pdcomponent.data_entity import DataEntity
from pdcomponent.device import Device
from pdcomponent.storekeeper import StoreKeeper
from itertools import product
from multiprocessing import Pool, cpu_count

import pandas as pd

from pdstats.data_import import DataSeries

"""
        non_seec_devices = [
            {
                'UMC 12AP1P2__CUB-3F__MTR06': [7060],
                'UMC 12AP1P2__CUB-3F__MTR07': [7061],
                'UMC 12AP1P2__CUB-3F__MTR08': [7062],
                'UMC 12AP1P2__CUB-3F__MTR05': [7072],
                'UMC 12AP1P2__CUB-3F__MTR09': [7090],
                'UMC 12AP1P2__CUB-3F__MTR13': [7135],
                'UMC 12AP1P2__CUB-3F__MTR12': [7136],
                'UMC 12AP1P2__CUB-3F__MTR11': [7137],
                'UMC 12AP1P2__CUB-3F__MTR72B': [7138],
                'UMC 12AP1P2__CUB-3F__MTR0A': [7139],
                'UMC 12AP1P2__CUB-3F__MTR04': [10028],
                'UMC 12AP1P2__CUB-3F__MTR01': [10030],
                'UMC 12AP1P2__CUB-3F__MTR03': [10032],
                'UMC 12AP1P2__CUB-3F__MTR02': [10034],
                'UMC 12AP1P2__CUB-3F__MTR51': [10058],

                'UMC 12AP1P2__SB-4F__STR71B': [6964],
                'UMC 12AP1P2__SB-4F__STR11': [6986],
                'UMC 12AP1P2__SB-4F__STR22': [7018],
                'UMC 12AP1P2__SB-4F__STR21': [7053],
                'UMC 12AP1P2__SB-F5__STR74B': [7067],
                'UMC 12AP1P2__P1-UB-2F__UTR11': [6970, 7109],
                'UMC 12AP1P2__P1-UB-2F__UTR21': [6972, 7142],
                'UMC 12AP1P2__P1-UB-2F__UTR22': [7144],
                'UMC 12AP1P2__P2-UB-2F__UTR75B': [7146],
                'UMC 12AP1P2__P2-UB-2F__UTR77B': [6966],
                'UMC 12AP1P2__P2-UB-2F__UTR76B': [6968],
                'UMC 12AP1P2__P2-UB-2F__UTR11B': [6970],
                'UMC 12AP1P2__P2-UB-2F__UTR21B': [6972],
                'UMC 12AP1P2__P2-UB-2F__UTR81B': [6974],
                'UMC 12AP1P2__P2-UB-2F__UTR73B': [6976],
                'UMC 12AP1P2__P2-UB-2F__UTR74B': [6978],
                'UMC 12AP1P2__P2-UB-2F__UTR13B': [6980],
                'UMC 12AP1P2__P2-UB-2F__UTR82B': [6982],
                'UMC 12AP1P2__P2-UB-2F__UTR83B': [6984],
                'UMC 12AP1P2__SB-4F__STR-51': [9996],
                'UMC 12AP1P2__SB-4F__STR-62': [9998],
                'UMC 12AP1P2__SB-4F__STR61': [10000],
                'UMC 12AP1P2__P1-UB-2F__UTR13': [6980, 15365],
                'UMC 12AP1P2__PSB__TR1': [6993, 6994],
                'UMC 12AP1P2__PSB__TR2': [6995, 6996],
                'UMC 12AP1P2__PSB__TR3': [7015, 7016],
                'UMC 12AP1P2__PSB__TR4': [7019, 7020],
                'UMC 12AP1P2__PSB__P1TR07': [8002],
                'UMC 12AP1P2__SB-F5__STR-52': [10037],
                'UMC 12AP1P2__P1-UB-2F__UTR-52': [10039],
                'UMC 12AP1P2__P1-UB-2F__UTR-61': [10045],
                'UMC 12AP1P2__P1-UB-2F__UTR-72': [10051],
                'UMC 12AP1P2__P1-UB-2F__UTR-64': [10055],
                'UMC 12AP1P2__P1-UB-2F__UTR-66': [10041],
                'UMC 12AP1P2__P1-UB-2F__UTR-51': [10047],
                'UMC 12AP1P2__P1-UB-2F__UTR-63': [10053],
                'UMC 12AP1P2__P1-UB-2F__UTR-65': [10057],
                'UMC 12AP1P2__P1-UB-2F__UTR-54': [10043],
                'UMC 12AP1P2__P1-UB-2F__UTR-67': [10049],

            },
            {
                'UMC 12AP3P4__PSB__MTR01-P': [6945, 6946, 6947],
                'UMC 12AP3P4__PSB__MTR01-S': [6948, 6949, 6950],
                'UMC 12AP3P4__PSB__MTR02-P': [6951, 6952, 6953],
                'UMC 12AP3P4__PSB__MTR02-S': [6954, 6955, 6956],
                'UMC 12AP3P4__CUB_3F__MTR381': [12882, 12883, 12884],
                'UMC 12AP3P4__CUB_3F__MTR382': [12885, 12886, 12887],
                'UMC 12AP3P4__CUB_3F__MTR384': [12888, 12889, 12890],
                'UMC 12AP3P4__CUB_3F__MTR385': [12891, 12892, 12893],
                'UMC 12AP3P4__CUB_3F__MTR383': [12894, 12895, 12896],
                'UMC 12AP3P4__CUB_3F__MTR387': [12897, 12898, 12899],
                'UMC 12AP3P4__CUB_3F__MTR386': [12900, 12901, 12902],
                'UMC 12AP3P4__CUB_3F__MTR388': [12903, 12904, 12905],
                'UMC 12AP3P4__SB_2F-S__STR301': [12918],
                'UMC 12AP3P4__SB_2F-S__STR302': [12923],
                'UMC 12AP3P4__SB_2F-S__STR303': [12928],
                'UMC 12AP3P4__SB_2F-S__STR304': [12933],
                'UMC 12AP3P4__SB_2F-S__STR309': [12938],
                'UMC 12AP3P4__SB_2F-N__STR305': [12943],
                'UMC 12AP3P4__SB_2F-N__STR306': [12948],
                'UMC 12AP3P4__SB_4F__STR307': [12953],
                'UMC 12AP3P4__SB_4F__STR308': [12958],

            },
            {
                'UMC 12AP5__CUB_P5_3F__MTRE102': [6688],
                'UMC 12AP5__CUB_P5_3F__MTRE202': [6689],
                'UMC 12AP5__CUB_P5_3F__MTRN102': [6690],
                'UMC 12AP5__CUB_P5_3F__MTRN202': [6691],
                'UMC 12AP5__CUB_P5_3F__MTRN103': [6692],
                'UMC 12AP5__CUB_P5_3F__MTRN203': [6693],
                'UMC 12AP5__CUB_P5_3F__MTRE103': [6694],
                'UMC 12AP5__CUB_P5_3F__MTRE203': [6695],
                'UMC 12AP5__CUB_P5_3F__MTRE106': [15369],
                'UMC 12AP5__CUB_P5_3F__MTRE206': [15370],
                'UMC 12AP5__CUB_P6_3F__MTRE302': [15826],
                'UMC 12AP5__CUB_P6_3F__MTRE402': [15827],
                'UMC 12AP5__CUB_P6_3F__MTRN302': [15828],
                'UMC 12AP5__CUB_P6_3F__MTRN402': [15829],
                'UMC 12AP5__CUB_P6_3F__MTRE303': [15830],
                'UMC 12AP5__CUB_P6_3F__MTRE403': [15831],
                'UMC 12AP5__CUB_P6_3F__MTRN303': [15832],
                'UMC 12AP5__CUB_P6_3F__MTRN403': [15833],
                'UMC 12AP5__CUB_P6_3F__MTRN406': [15834],
                'UMC 12AP5__CUB_P6_3F__MTRN306': [15835],
                #
                'UMC 12AP5__PSB_P5_1F__TR04P': [15808],
                'UMC 12AP5__PSB_P5_1F__TR03P': [15810],
                'UMC 12AP5__PSB_P5_1F__161KV_TR1': [6686],
                'UMC 12AP5__PSB_P5_1F__TR1': [6764, 6765],
                'UMC 12AP5__PSB_P5_1F__TR2': [6766, 6767],
                'UMC 12AP5__PSB_P5_2F__TR514': [6775],
                'UMC 12AP5__PSB_P5_1F__TR04S': [15809],
                'UMC 12AP5__PSB_P5_1F__TR03S': [15811],
                'UMC 12AP5__PSB_P5_1F__161KV_TR2': [6687],
                'UMC 12AP5__FAB_P5_W_1F__FTRN210': [6719],
                'UMC 12AP5__FAB_P5_W_1F__FTRN110': [6724],
                'UMC 12AP5__FAB_P5_W_1F__FTRE204': [6725],
                'UMC 12AP5__FAB_P5_W_1F__FTRE104': [6731],
                'UMC 12AP5__FAB_P5_W_1F__FTRN203': [6733],
                'UMC 12AP5__FAB_P5_W_1F__FTRN103': [6734],
                'UMC 12AP5__FAB_P5_W_1F__FTRE203': [6735],
                'UMC 12AP5__FAB_P5_W_1F__FTRE103': [6737],
                'UMC 12AP5__FAB_P5_E_1F__FTRE109': [15362],
                'UMC 12AP5__FAB_P5_E_1F__FTRE209': [15363],
                'UMC 12AP5__FAB_P5_2F__FTRN106': [6739],
                'UMC 12AP5__FAB_P5_2F__FTRN206': [6741],
                'UMC 12AP5__FAB_P5_2F__FTRE206': [6743],
                'UMC 12AP5__FAB_P5_2F__FTRE106': [6749],
                'UMC 12AP5__FAB_P5_4F__FTRN107': [6751],
                'UMC 12AP5__FAB_P5_4F__FTRN207': [6753],
                'UMC 12AP5__FAB_P5_4F__FTRE207': [6755],
                'UMC 12AP5__FAB_P5_4F__FTRE107': [6757],
                'UMC 12AP5__OB_P5_5F__OTRN204': [6759],
                'UMC 12AP5__OB_P5_5F__OTRE104': [6761],
                'UMC 12AP5__FAB_P6_2F__FTRN409': [16174, 16175, 16176],
                'UMC 12AP5__FAB_P6_2F__FTRE307': [16178, 16179, 16180],
                'UMC 12AP5__FAB_P6_2F__FTRN309': [16182, 16183, 16184],
                'UMC 12AP5__FAB_P6_2F__FTRE407': [16186, 16187, 16188],
                'UMC 12AP5__FAB_P6_4F__FTRE302': [16189, 16190, 16191],
                'UMC 12AP5__FAB_P6_4F__FTRE402': [16192, 16193, 16194],
                'UMC 12AP5__FAB_P6_4F__FTRN402': [16195, 16196, 16197],
                'UMC 12AP5__FAB_P6_4F__FTRN302': [16200, 16201, 16202],
                'UMC 12AP5__OB_P6_5F__OTRN104': [16205, 16206, 16207],
                'UMC 12AP5__FAB_P5_E_1F__FTRE101': [6800],
                'UMC 12AP5__FAB_P5_E_1F__FTRN102': [6802],
                'UMC 12AP5__FAB_P5_E_1F__FTRN104': [6804],
                'UMC 12AP5__FAB_P5_E_1F__FTRN105': [10082],
                'UMC 12AP5__FAB_P5_E_1F__FTRN108': [10084],
                'UMC 12AP5__FAB_P5_E_1F__FTRN109': [10086],
                'UMC 12AP5__FAB_P5_E_1F__FTRN101': [15263],
                'UMC 12AP5__FAB_P5_E_1F__FTRN111': [15354],
                'UMC 12AP5__FAB_P5_E_1F__FTRE102': [15352],
                'UMC 12AP5__FAB_P6_1F_E側__FTRE401': [16211, 16212, 16213],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN401': [16221, 16222, 16223],
                'UMC 12AP5__OB_P6_5F__OTRE204': [16208, 16209, 16210],
                'UMC 12AP5__FAB_P5_E_1F__FTRE201': [6801],
                'UMC 12AP5__FAB_P5_E_1F__FTRN202': [6803],
                'UMC 12AP5__FAB_P5_E_1F__FTRN204': [6805],
                'UMC 12AP5__FAB_P5_E_1F__FTRN205': [10083],
                'UMC 12AP5__FAB_P5_E_1F__FTRN208': [10085],
                'UMC 12AP5__FAB_P5_E_1F__FTRN209': [10087],
                'UMC 12AP5__FAB_P5_E_1F__FTRN201': [15264],
                'UMC 12AP5__FAB_P5_E_1F__FTRN211': [15355],
                'UMC 12AP5__FAB_P5_E_1F__FTRE202': [15353],
                'UMC 12AP5__FAB_P6_1F_E側__FTRE301': [16214, 16215, 16216],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN301': [16224, 16225, 16226],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN602': [16228, 16229, 16230],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN408': [16232, 16233, 16234],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN404': [16236, 16237, 16238],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN304': [16242, 16243, 16244],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN502': [16246, 16247, 16248],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN403': [16252, 16253, 16254],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN308': [16239, 16240, 16250],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN406': [16256, 16257, 16258],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN601': [16260, 16261, 16262],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN303': [16264, 16265, 16266],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN306': [16268, 16269, 16270],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN501': [16272, 16273, 16274],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN503': [16276, 16277, 16278],
                'UMC 12AP5__FAB_P6_1F_E側__FTRE303': [16280, 16281, 16282],
                'UMC 12AP5__FAB_P6_1F_E側__FTRE306': [16284, 16285, 16286],
                'UMC 12AP5__FAB_P6_1F_E側__FTRE406': [16288, 16289, 16290],
                'UMC 12AP5__FAB_P6_1F_E側__FTRE403': [16292, 16293, 16294],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN603': [16296, 16297, 16298],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN507': [16300, 16301, 16302],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN607': [16304, 16305, 16306],
                'UMC 12AP5__FAB_P6_1F_E側__FTRE304': [16308, 16309, 16310],
                'UMC 12AP5__FAB_P6_1F_E側__FTRE404': [16312, 16313, 16314],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN604': [16316, 16317, 16318],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN504': [16320, 16321, 16322],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN605': [16324, 16325, 16326],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN505': [16328, 16329, 16330],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN606': [16332, 16333, 16334],
                'UMC 12AP5__FAB_P6_1F_E側__FTRN506': [16336, 16337, 16338],

            },
            {
                'UMC 8AB__MTR_MTR1': [6868],
                'UMC 8AB__MTR_MTR2': [6869],
                'UMC 8AB__MTR_MTR3': [6870],
                'UMC 8AB__MTR_MTR4': [6871],
                'UMC 8AB__M1F_MTR213': [6872],
                'UMC 8AB__M1F_MTR302': [11452],
                'UMC 8AB__M1F_MTR402': [11453],
                'UMC 8AB__M1F_MTR703': [11454],
                'UMC 8AB__M1F_MTR503': [16417],
                'UMC 8AB__M1F_MTR301': [16592],
                'UMC 8AB__M1F_MTR401': [17352],
                'UMC 8AB__M1F_MTR108': [17353],

                'UMC 8AB__161開關廠_1F__16TR04': [6840],
                'UMC 8AB__SBF電氣室__BTR206': [11448],
                'UMC 8AB__SBF電氣室__BTR107': [11451],
                'UMC 8AB__SBF電氣室__BTR207': [11528],
                'UMC 8AB__SBF電氣室__BTR702': [11531],
                'UMC 8AB__SBF電氣室__BTR502': [11534],
                'UMC 8AB__SBF電氣室__BTR106': [11537],
                'UMC 8AB__SBF電氣室__BTR111': [11540],
                'UMC 8AB__O5F電氣室__OTR105': [11546],
                'UMC 8AB__O5F電氣室__OTR205': [11549],

            },
            {
                'UMC 8CD__CUB變電站__MTR01': [10767],
                'UMC 8CD__CUB變電站__MTR02': [10768],
                'UMC 8CD__CUB變電站__MTR03': [10769],
                'UMC 8CD__CUB變電站__MTR04': [10770],
                'UMC 8CD__CUB變電站__MTR05': [10771],
                'UMC 8CD__CUB變電站__MTR06': [10772],
                'UMC 8CD__CUB變電站__MTR07': [10773],
                'UMC 8CD__CUB變電站__MTR08': [10774],
                'UMC 8CD__GB變電站__MTR11': [10793],
                'UMC 8CD__GB變電站__MTR12': [10795],
                'UMC 8CD__GB變電站__MTR13': [10797],
                'UMC 8CD__GB變電站__MTR14': [10799],
                'UMC 8CD__GB變電站__MTR15': [10801],
                'UMC 8CD__GB變電站__MTR16': [10803],
                # 移除管溝部分

                'UMC 8CD__8C_Station__CTR-01': [10179],
                'UMC 8CD__8C_Station__CTR-02': [10180],
                'UMC 8CD__8C_Station__CTR-04': [10181],
                'UMC 8CD__SB_Station__STR-01': [10186],
                'UMC 8CD__SB_Station__STR-03': [10187],
                'UMC 8CD__UT_Station__OTR-01': [10188],
                'UMC 8CD__8C_Station__CTR-03': [11256],
                'UMC 8CD__8C_Station__CTR-05': [11259],
                'UMC 8CD__8C_Station__CTR-08': [11265],
                'UMC 8CD__8C_Station__CTR-11': [11332],
                'UMC 8CD__8C_Station__CTR-14': [11268],
                'UMC 8CD__8D_Station__DTR-01': [11277],
                'UMC 8CD__8D_Station__DTR-05': [11280],
                'UMC 8CD__8D_Station__DTR-06': [11283],
                'UMC 8CD__8D_Station__DTR-10': [11286],
                'UMC 8CD__UT_Station__OTR-04': [11295],
                'UMC 8CD__SB_Station__STR-05': [11302],
                'UMC 8CD__SB_Station__STR-13': [11313],

            },
            {
                'UMC 8F__CUB__MTR02': [10205],
                'UMC 8F__CUB__MTR03': [10206],
                'UMC 8F__CUB__MTR04': [10207],
                'UMC 8F__CUB__MTR05': [10208],
                'UMC 8F__CUB__MTR06': [10209],
                'UMC 8F__CUB__MTR01': [10235],
                'UMC 8F__CUB__MTR09': [12232]
            },
            {
                'UMC 8F__#1_SS__MTR1_HV': [12364],
                'UMC 8F__#1_SS__MTR1_LV': [12365],
                'UMC 8F__#1_SS__MTR2_HV': [12366],
                'UMC 8F__#1_SS__MTR2_V': [12367],

                'UMC 8F__S-FAB__FTR01': [10220],
                'UMC 8F__S-FAB__FTR04': [10221],
                'UMC 8F__S-FAB__FTR03': [10222],
                'UMC 8F__S-FAB__FTR09': [10223],
                'UMC 8F__S-FAB__FTR05': [10226],
                'UMC 8F__SB__STR01': [10210],
                'UMC 8F__SB__STR03': [10211],
                'UMC 8F__SB__STR02': [10212],
                'UMC 8F__SB__STR04': [10213],

            },
            {
                'UMC SG__CUB3F__MTR103': [7499],
                'UMC SG__CUB3F__MTR102': [7500],
                'UMC SG__CUB3F__MTR205': [7513],
                'UMC SG__CUB3F__MTR206': [7514],
                'UMC SG__CUB3F__MTR204': [7533],
                'UMC SG__CUB3F__MTR203': [7540],
                'UMC SG__CUB3F__MTR302': [7550],
                'UMC SG__CUB3F__MTR306': [7551],
                'UMC SG__CUB3F__MTR305': [7552],
                'UMC SG__CUB3F__MTR202': [7602],
                'UMC SG__CUB3F__MTR201': [7619],
                'UMC SG__CUB3F__MTR301': [7620],
                'UMC SG__CUB3F__MTR303': [7621],
                'UMC SG__CUB3F__MTR304': [7622],
                'UMC SG__CUB3F__MTR308': [7623],
                'UMC SG__CUB3F__MTR104': [7797],
                'UMC SG__CUB3F__MTR101': [7798],
                'UMC SG__CUB3F__MTR207': [14639],
                'UMC SG__CUB3F__MTR208': [14642],
                'UMC SG__CUB3F__MTR501': [14656],
                'UMC SG__CUB3F__MTR502': [14657],
                'UMC SG__CUB3F__MTR504': [14658]
            },
            {
                'USC 12x__3.2M-L03-HV__FTRN211': [9405],
                'USC 12x__3.2M-L03-HV__FTRE111': [9406],
                'USC 12x__3.2M-L03-HV__FTRE101': [9407],
                'USC 12x__3.2M-L03-HV__FTRE201': [9408],
                'USC 12x__3.2M-L03-HV__FTRN201': [9409],
                'USC 12x__3.2M-L03-HV__FTRN101': [9410],
                'USC 12x__3.2M-L03-HV__FTRN204': [9429],
                'USC 12x__3.2M-L03-HV__FTRN104': [9431],
                'USC 12x__3.2M-L03-HV__FTRN203': [9432],
                'USC 12x__3.2M-L03-HV__FTRN103': [9433],
                'USC 12x__3.2M-L03-HV__FTRE202': [9437],
                'USC 12x__3.2M-L03-HV__FTRN202': [9438],
                'USC 12x__3.2M-L03-HV__FTRN102': [9439],
                'USC 12x__3.2M-L03-HV__FTRE103': [9443],
                'USC 12x__3.2M-L03-HV__FTRE203': [9444],
                'USC 12x__3.2M-L03-HV__FTRE102': [9445],
                'USC 12x__4F-L40-HV__FTRE212': [9449],
                'USC 12x__4F-L40-HV__FTRE112': [9450],
                'USC 12x__4F-L40-HV__FTRN212': [9451],
                'USC 12x__4F-L40-HV__FTRN112': [9455],
                'USC 12x__4F-L40-HV__FTRS101': [13823],
                'USC 12x__OB-5F-HV__OTRN204': [9457],
                'USC 12x__OB-5F-HV__OTRN104': [9459],
                'USC 12x__PB-3F-HV__PTR114': [9482],
                'USC 12x__3.2M-L03-HV__FTRE204': [9483],
                'USC 12x__3.2M-L03-HV__FTRE104': [9484],
                'USC 12x__3.2M-L03-HV__FTRN105': [9485],
                'USC 12x__3.2M-L03-HV__FTRN205': [9486],
                'USC 12x__3.2M-L03-HV__FTRN206': [9487],
                'USC 12x__3.2M-L03-HV__FTRN106': [9488],
                'USC 12x__3.2M-L03-HV__FTRN108': [9489],
                'USC 12x__3.2M-L03-HV__FTRN208': [9490],
                'USC 12x__3.2M-L03-HV__FTRN107': [9509],
                'USC 12x__3.2M-L03-HV__FTRN207': [9510],
                'USC 12x__3.2M-L03-HV__FTRN109': [9511],
                'USC 12x__3.2M-L03-HV__FTRE209': [13261],
                'USC 12x__C1-3F-HV__MTRN104': [13262],
                'USC 12x__C1-3F-HV__MTRN204': [13263],
                'USC 12x__C1-3F-HV__MTRE204': [13264],
                'USC 12x__C1-3F-HV__MTRE104': [13269],
                'USC 12x__C1-3F-HV__MTRN103': [13270],
                'USC 12x__C1-3F-HV__MTRN203': [13825],
                'USC 12x__C1-3F-HV__MTRE203': [13914],
                'USC 12x__C1-3F-HV__MTRE103': [13915],
                'USC 12x__C1-3F-HV__MTRN102': [13916],
                'USC 12x__C1-3F-HV__MTRE202': [13917],
                'USC 12x__C1-3F-HV__MTRE102': [14873],
                'USC 12x__C1-3F-HV__MTRS101': [14874],
                'USC 12x__C1-3F-HV__MTRE205': [16389],
                'USC 12x__C1-3F-HV__MTRE105': [16390],
                'USC 12x__C1-3F-HV__MTRN205': [16391],
                'USC 12x__C1-3F-HV__MTRN105': [16392],
                'USC 12x__GM-1F-HV__GMTR101': [15380]
            },
            {

                'UMC 8S__#2_S/S__TR-11A': [12276],
                'UMC 8S__#2_S/S__TR-11C': [12278],
                'UMC 8S__#2_S/S__TR-11E': [12280],
                'UMC 8S__#2_S/S__TR-11G': [12282],
                'UMC 8S__#2_S/S__TR-11I': [12284],
                'UMC 8S__#2_S/S__TR-11K': [12286],
                'UMC 8S__#2_S/S__TR-21B': [12289],
                'UMC 8S__#2_S/S__TR-21D': [12291],
                'UMC 8S__#2_S/S__TR-21F': [12293],
                'UMC 8S__#2_S/S__TR-21H': [12295],
                'UMC 8S__#2_S/S__TR-21J': [12297],
                'UMC 8S__#2_S/S__TR-E12A1': [12299],
                'UMC 8S__#2_S/S__TR-E12A2': [12301],
                'UMC 8S__#3_S/S__TR-E12B1': [12332],
                'UMC 8S__#3_S/S__TR-13A': [12334],
                'UMC 8S__#3_S/S__TR-23B': [12336],
                'UMC 8S__#7_S/S__TR-21L': [12337],
                'UMC 8S__#5_S/S__TR-#1': [12339],
                'UMC 8S__#2_S/S__TR-21A': [12277],
                'UMC 8S__#2_S/S__TR-21C': [12279],
                'UMC 8S__#2_S/S__TR-21E': [12281],
                'UMC 8S__#2_S/S__TR-21G': [12283],
                'UMC 8S__#2_S/S__TR-21I': [12285],
                'UMC 8S__#2_S/S__TR-21K': [12287],
                'UMC 8S__#2_S/S__TR-11B': [12290],
                'UMC 8S__#2_S/S__TR-11D': [12292],
                'UMC 8S__#2_S/S__TR-11F': [12294],
                'UMC 8S__#2_S/S__TR-11H': [12296],
                'UMC 8S__#2_S/S__TR-11J': [12298],
                'UMC 8S__#2_S/S__TR-E22A1': [12300],
                'UMC 8S__#2_S/S__TR-E22A2': [12302],
                'UMC 8S__#3_S/S__TR-E22B1': [12333],
                'UMC 8S__#3_S/S__TR-23A': [12335],
                'UMC 8S__#7_S/S__TR-U14': [12338],
                'UMC 8S__#5_S/S__TR-#2': [12340],
                'UMC 8S__#7_S/S__TR-13B': [12347]
            },
            {
                '和艦__CUB1F__MTR-04': [13090],
                '和艦__CUB1F__MTR-01': [13186],
                '和艦__CUB1F__MTR-03': [13216],
                '和艦__CUB1F__MTR-02': [13236],
                '和艦__CUB1F__MTR05': [14492]
            },
            {

                'UMC 8E__U棟2F模鑄TR__UTR01': [12386],
                'UMC 8E__U棟2F模鑄TR__UTR03': [12388],
                'UMC 8E__U棟2F模鑄TR__UTR05': [12390],
                'UMC 8E__U棟2F模鑄TR__UTR07': [12392],
                'UMC 8E__U棟2F模鑄TR__UTR09': [12394],
                'UMC 8E__U棟2F模鑄TR__UTR11': [12396],
                'UMC 8E__U棟2F模鑄TR__UTR13': [12398],
                'UMC 8E__U棟3F模鑄TR__UTR15': [12404],
                'UMC 8E__U棟3F模鑄TR__UTR17': [12406],
                'UMC 8E__FB棟模鑄TR__FTR01': [12422],
                'UMC 8E__FB棟模鑄TR__FTR03': [12424],
                'UMC 8E__SB棟模鑄TR__STR01': [12434],
                'UMC 8E__SB棟模鑄TR__STR03': [12438],
                'UMC 8E__U棟2F模鑄TR__UTR02': [12387],
                'UMC 8E__U棟2F模鑄TR__UTR04': [12389],
                'UMC 8E__U棟2F模鑄TR__UTR06': [12391],
                'UMC 8E__U棟2F模鑄TR__UTR08': [12393],
                'UMC 8E__U棟2F模鑄TR__UTR10': [12395],
                'UMC 8E__U棟2F模鑄TR__UTR12': [12397],
                'UMC 8E__U棟2F模鑄TR__UTR14': [12399],
                'UMC 8E__U棟3F模鑄TR__UTR16': [12405],
                'UMC 8E__U棟3F模鑄TR__UTR18': [12407],
                'UMC 8E__FB棟模鑄TR__FTR02': [12423],
                'UMC 8E__FB棟模鑄TR__FTR04': [12425],
                'UMC 8E__SB棟模鑄TR__STR02': [12435],
                'UMC 8E__SB棟模鑄TR__STR04': [12439]

            }
        ]
"""


def month_slot(alarm_map, nano):
    return ((alarm_map[str(nano)].naTime.year - 2023)*12 + (alarm_map[str(nano)].naTime.month - 6))


def packing_dataframe(device_instance: Device, pre_evaluating_mv=5):

    print(device_instance.gNo, device_instance.gName, device_instance.begin_datetime, device_instance.ending_datetime)

    ds_dict = dict()

    for idx, ch in enumerate(device_instance.gChannel):
        data = list()
        pdm: DataEntity
        data.append(["{}_CH{}".format(device_instance.gName, ch), "", ""])
        data.append(["timestamp", "Magnitude", "Count"])
        try:
            for row_count, pdm in enumerate(device_instance.trend_data.get(ch)):
                data.append([pdm.bdTime, pdm.bdMV, pdm.bdCount])
        except TypeError:
            print("Ignore.")
        df = pd.DataFrame(data)
        ds = DataSeries(df, 0, device_name=device_instance.gName, station_name=device_instance.sName)
        ds.channel = ch
        ds.gno, ds.sno, ds.sname = device_instance.gNo, device_instance.sNo_link, device_instance.sName
        try:
            ds.analyze_by_specific_voltage(pre_evaluating_mv)
        except IndexError:
            print("Ignore.")
        ds.report()
        ds_dict[ch] = ds

    return ds_dict


def evaluating_thresholding(pickle_path: Path, evaluating_duration_minutes: list, evaluating_mini_volt: list):
    csv_rows = []

    ratio_list = [0]
    ds_dict: dict
    sk = StoreKeeper()
    dev: Device
    dev = sk.load_pickle_object(pickle_path)
    # 將 DEVICE.begin_datetime 與 DEVICE.ending_datetime 限制成嚴格 ISO8601 短格式
    tz_info = timezone(timedelta(hours=8, minutes=0))
    dev.begin_datetime.astimezone(tz_info)
    dev.ending_datetime.astimezone(tz_info)

    dt_begin = dev.begin_datetime.isoformat()
    dt_end = dev.ending_datetime.isoformat()
    target = dev.gNo

    try:
        ds_dict = packing_dataframe(device_instance=dev, pre_evaluating_mv=5)
    except ValueError as e:
        print("gNo={}, sta={}, {} loading data exception, ignored continue to next device."
              .format(target, dev.sName, dev.gName))
        logging.error("gNo={}, sta={}, {} loading data exception, ignored continue to next device.",
                      target, dev.sName, dev.gName)
        logging.error(e)

    voltage_mean = dict()
    voltage_std = dict()

    for idx, channel in enumerate(dev.gChannel):
        ds = ds_dict[channel]
        ds.ratio_list = ratio_list
        ds.voltage_threshold_min = 1
        ds.lasting_minutes_min = 3
        print(ds)
        ds.report()

        voltage_mean[channel] = float(f"{ds.voltage.mean():.1f}")
        voltage_std[channel] = float(f"{ds.voltage.std():.1f}")

    print(dev.sName + "__" + dev.gName)

    for evaluating_mv in evaluating_mini_volt:
        for ch_idx, channel in enumerate(dev.gChannel):
            ds = ds_dict[channel]

            for min_idx, _min_ in enumerate(sorted(evaluating_duration_minutes)):
                dt_shift = timedelta(minutes=_min_)
                try:
                    ds.analyze_by_specific_voltage(evaluating_mv)
                    o, occurrence_info = ds.count_occurrence(_min_)
                except IndexError:
                    csv_rows.append([dev.gNo, dt_begin, dt_end,
                                         channel,
                                         voltage_mean[channel],
                                         voltage_std[channel],
                                         str(evaluating_mv) + "mv",
                                         str(_min_) + "min", "-1"])
                    continue

                try:
                    csv_rows.append([dev.gNo, dt_begin, dt_end,
                                     channel,
                                     voltage_mean[channel],
                                     voltage_std[channel],
                                     str(evaluating_mv) + "mv",
                                     str(_min_) + "min"] + [
                                        (dt_pair[0] + dt_shift).isoformat(timespec="seconds") for dt_pair in
                                        occurrence_info])
                except IndexError as e:
                    print(e)
            del ds

    return csv_rows


class TestUMC8SAnalysis(unittest.TestCase):
    def test_dump_global_seec_tr(self):
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

        tz = timezone(timedelta(hours=+8))
        sk = StoreKeeper()

        for idx, dev in enumerate(target_list):
            print("Loading {} - {}, {:2.0f}%".format(dev.gNo, dev.gName, (idx+1)*100/len(target_list)))
            dev.begin_datetime = datetime.datetime(2023, 6, 1, 0, 0, 0,tzinfo=tz)
            dev.ending_datetime = datetime.datetime(2024, 5, 31, 0, 0, 0, tzinfo=tz)
            #dev.load_trend_data()
            #sk.pickle_device_data(dev)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def parsing_csv(rows: pd.DataFrame):
    tr_review = dict()
    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    ym_tag_list: list[str] = []
    for idx, row in rows.iterrows():
        gno = int(row['gno'])
        channel_index = int(str(row['ch']))
        th_mv, th_min = row['th_mv'], row['th_min']
        t = Triggering(gno, channel_index, int(str(row['th_mv'])[:-2]), int(str(row['th_min'])[:-3]),
                       mean=float(row['mean']), std=float(row['std']))

        ts_begin = datetime.datetime.fromisoformat(row['dt_begin'])
        ts_end = datetime.datetime.fromisoformat(row['dt_end'])

        ym_tag_list = list(t.build_year_month_bins(begin=ts_begin, end=ts_end))

        tr_review['{}_{}_{}mv_{}min'.format(gno, channel_index, th_mv, th_min)] = []

        for event in row.iloc[8:]:
            if event == '-1' or event == '-1.0' or type(event) is bool or (type(event) is float and math.isnan(event)):
                continue
            # 2024-01-21T23:37:34
            try:
                t.append(datetime.datetime.strptime(event, dt_fmt))
            except TypeError:
                continue

        tr_review['{}_{}_{}mv_{}min'.format(gno, channel_index, th_mv, th_min)].append(t)

    cuno_grouping = dict()
    customer_info = [u.__dict__ for u in session.query(MysqlCustomer).all()]
    devices_overview = []
    for r in session.query(MysqlOverviewEntity).all():
        if r is not None:
            devices_overview.append(r.__dict__)

    for idx, tr_list in tr_review.items():
        tr: Triggering
        for tr in tr_list:  # if tr.check_over_triggering():
            gno = tr.gno
            dev_info = next(filter(lambda x: x['gNo'] == gno, devices_overview))
            tr.cuNo = dev_info['cuNo_link']
            cu_name = full2half(next(filter(lambda x: x['cuNo'] == tr.cuNo, customer_info))['cuName'])
            tr.cuName = str.strip(cu_name)
            tr.sName = full2half(str.strip(dev_info['sName']))
            tr.gName = full2half(str.strip(dev_info['gName']))

            if tr.cuNo not in cuno_grouping.keys():
                cuno_grouping[tr.cuNo] = CustomerGrouping(tr.cuNo, cu_name)

            grouping = cuno_grouping[tr.cuNo]
            grouping.append(tr)
            # print(tr)
    print("===================================================================================")
    header = CustomerGrouping.csv_column_header(ym_tag_list)
    csv_rows = [elem.dump_csv_format() for k, elem in cuno_grouping.items()]
    print(header + csv_rows)
    return header, csv_rows


def multiprocess_evaluation(dt_begin, dt_end, output_base_file_dict, pickle_path, pool, thres_duration_steps,
                            thres_voltage_steps):
    """  分配多行程進行平行計算 evaluating_thresholding  """
    # column_names = ['gno', 'dt_begin', 'dt_end', 'ch', 'avg', 'std', 'th_mv', 'th_min'] + [觸發事件資料]
    for list_tag, group_list in output_base_file_dict.items():
        args = product([pickle_path / "{}_{}_{}.pickle".format(elm, dt_begin, dt_end)
                        for elm in group_list], [thres_voltage_steps], [thres_duration_steps])
        pool_output = pool.starmap_async(evaluating_thresholding, args, chunksize=24)

        csv_rows = pool_output.get()
        csv_file = 'occurrence_info_{}.csv'.format(list_tag)

        with open(csv_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar="'")
            for rec in csv_rows:
                for row in rec:
                    writer.writerow(row)

        print(list_tag, csv_file, "FINISH!")

        devices_map = dict()
        for gno in group_list:
            dev = Device(gno)
            # list 方便後續將多通道整併 例如 RST 分開編號的
            devices_map["{}__{}__{}".format(dev.cuName, dev.sName, dev.gName)] = [gno]

        # ===========================================================================================================
        """ 將 csv 轉換成 DataFrame 並給予 欄位名稱 方便後續分析 """
        csv_file = 'occurrence_info_umc_8s.csv'
        # https://stackoverflow.com/a/57824142 當預設欄位太少不足以完成自動解析時  將會觸發記憶體不足 可以直接指定一個較大的數值
        column_names = ['gno', 'dt_begin', 'dt_end', 'ch', 'mean', 'std', 'th_mv', 'th_min'] + [i for i in range(1, 50993)]
        # 主要需要手動確認是否有一個裝置散佈在多個通道群 (即 RST 三相分開編號) 有的話手動合併 RST 於一個裝置名稱內

        rows = pd.read_csv(csv_file, header=None, delimiter=',', quotechar="'", names=column_names)
        report = []

        for dev_name, gno_list in devices_map.items():
            r = []
            for gno in gno_list:
                r.append(
                    rows.assign(if_in_gno_lis=lambda df: df['gno'] == gno)
                    .loc[lambda df: df['if_in_gno_lis'],]
                )

            report.append(pd.concat(r, axis=0, ignore_index=True).sort_values(by=['th_mv', 'th_min', 'gno'],
                                                                              ignore_index=True))

        # ===========================================================================================================
        f = pd.concat(report, axis=0, ignore_index=True)
        print(f.shape)
        with open(Path(csv_file + ".pickle"), 'wb') as pf:
            pickle.dump(f, pf)

        with open(Path(csv_file+'.pickle'), 'rb') as pf:
            rows = pickle.load(pf)
            csv_header, csv_rows = parsing_csv(rows)
            with open(Path(csv_file.replace('.csv', '_report.csv')), 'w', newline='') as rpt_file:
                writer = csv.writer(rpt_file, delimiter=',', quotechar="'")
                for cu in csv_rows:
                    for dev in cu:
                        writer.writerow(dev)


class TestDumpDevicePickles(unittest.TestCase):

    def test_README(self):
        """
            1. 從 PDCare MySQL                        下載對應裝置分析區間的 PICKLE 檔案, 檔案名稱由 gno_{開始時間}_{結束時間} 表示
            2. 執行 test_load_pickle_analysis         進行不同閾值的觸發次數評估清單 產生 occurrence_info_xxxx.csv
                                                     將 occurrence_info_xxxx.csv 進行年月次數統計彙整 並給予欄位名稱轉換成 Pandas DataFrame
                                                     轉換成 DataFrame 並給予蘭為方便後續分析
                                                     將 pickled DataFrame 進行統計產出 後綴 report 的 CSV 檔案
        """

    def test_dump(self):
        tz = timezone(timedelta(hours=+8))
        sk = StoreKeeper()

        target_list = [
            # 12AP3P4
            Device(12870), Device(12871), Device(12872)
        ]

        for dev in target_list:
            dev.begin_datetime = datetime.datetime(2023, 6, 1, 0, 0, 0,tzinfo=tz)
            dev.ending_datetime = datetime.datetime(2024, 5, 31, 0, 0, 0, tzinfo=tz)
            dev.load_trend_data()
            sk.pickle_device_data(dev)

    def test_load_pickle_analysis(self):
        pool = Pool(cpu_count() - 1, maxtasksperchild=10)
        pickle_path = Path("e:\\src\\pdstats\\data")
        dt_begin = "20230601000000"
        dt_end = "20240531000000"

        thres_voltage_steps = [5, 10, 15, 20, 25, 30]
        thres_duration_steps = [10, 20, 30, 40, 50, 60]

        # 存放所有需要分析的廠區設備清單, key: 客戶名稱, val: 設備列表
        output_base_file_dict = {'umc_8s': [
            12276, 12277, 12278, 12279, 12280, 12281, 12282, 12283, 12284, 12285,
            12286, 12287, 12289, 12290, 12291, 12292, 12293, 12294, 12295, 12296,
            12297, 12298, 12299, 12300, 12301, 12302, 12332, 12333, 12334, 12335,
            12336, 12337, 12338, 12339, 12340, 12347, 12364, 12365, 12366, 12367
        ]}

        multiprocess_evaluation(dt_begin, dt_end, output_base_file_dict, pickle_path, pool, thres_duration_steps,
                                thres_voltage_steps)

    def test_make_report_csv(self):
        seec_group_list = [
            # UMC 8S
            12276, 12277, 12278, 12279, 12280, 12281, 12282, 12283, 12284, 12285,
            12286, 12287, 12289, 12290, 12291, 12292, 12293, 12294, 12295, 12296,
            12297, 12298, 12299, 12300, 12301, 12302, 12332, 12333, 12334, 12335,
            12336, 12337, 12338, 12339, 12340, 12347, 12364, 12365, 12366, 12367
        ]
        output_base_file_dict = {'umc_8s': seec_group_list}
        csv_file = 'occurrence_info_{}.csv'.format(list(output_base_file_dict.keys())[0])
        with open(Path(csv_file+'.pickle'), 'rb') as pf:
            rows = pickle.load(pf)
            csv_header, csv_rows = parsing_csv(rows)
            with open(Path(csv_file.replace('.csv', '_report.csv')), 'w', newline='') as rpt_file:
                writer = csv.writer(rpt_file, delimiter=',', quotechar="'")
                for cu in csv_rows:
                    for dev in cu:
                        writer.writerow(dev)

    def test_redo_analysis(self):
        csv_file = 'occurrence_info_umc_8s.csv'
        with open(Path(csv_file+'.pickle'), 'rb') as pf:
            rows = pickle.load(pf)
            csv_header, csv_rows = parsing_csv(rows)
            with open(Path(csv_file.replace('.csv', '_report.csv')), 'w', newline='', encoding='utf-8') as rpt_file:
                writer = csv.writer(rpt_file, delimiter=',')
                for cu in csv_rows:
                    for dev in cu:
                        writer.writerow(dev)

    def test_pickle_csv(self):
        """ 列印裝置通道整併清單 """
        gno_list = [
            12276, 12277, 12278, 12279, 12280, 12281, 12282, 12283, 12284, 12285,
            12286, 12287, 12289, 12290, 12291, 12292, 12293, 12294, 12295, 12296,
            12297, 12298, 12299, 12300, 12301, 12302, 12332, 12333, 12334, 12335,
            12336, 12337, 12338, 12339, 12340, 12347, 12364, 12365, 12366, 12367]

        devices_map = dict()
        for gno in gno_list:
            dev = Device(gno)
            # list 方便後續將多通道整併 例如 RST 分開編號的
            devices_map["{}__{}__{}".format(dev.cuName, dev.sName, dev.gName)] = [gno]

        """ 將 csv 轉換成 DataFrame 並給予蘭為方便後續分析 """
        csv_file = 'occurrence_info_umc_8s.csv'
        # https://stackoverflow.com/a/57824142 當預設欄位太少不足以完成自動解析時  將會觸發記憶體不足 可以直接指定一個較大的數值
        column_names = ['gno', 'dt_begin', 'dt_end', 'ch', 'mean', 'std', 'th_mv', 'th_min'] + [i for i in range(1, 50993)]
        # 主要需要手動確認是否有一個裝置散佈在多個通道群 (即 RST 三相分開編號) 有的話手動合併 RST 於一個裝置名稱內

        rows = pd.read_csv(csv_file, header=None, delimiter=',', quotechar="'", names=column_names)
        report = []

        for dev_name, gno_list in devices_map.items():
            r = []
            for gno in gno_list:
                r.append(
                    rows.assign(if_in_gno_lis=lambda df: df['gno'] == gno)
                    .loc[lambda df: df['if_in_gno_lis'],]
                )

            report.append(pd.concat(r, axis=0, ignore_index=True).sort_values(by=['th_mv', 'th_min', 'gno'],
                                                                              ignore_index=True))

        f = pd.concat(report, axis=0, ignore_index=True)
        print(f.shape)
        with open(Path(csv_file + ".pickle"), 'wb') as pf:
            pickle.dump(f, pf)

    def test_eval(self):
        # UMC 8S
        gno_list = [
            12276, 12277, 12278, 12279, 12280, 12281, 12282, 12283, 12284, 12285,
            12286, 12287, 12289, 12290, 12291, 12292, 12293, 12294, 12295, 12296,
            12297, 12298, 12299, 12300, 12301, 12302, 12332, 12333, 12334, 12335,
            12336, 12337, 12338, 12339, 12340, 12347, 12364, 12365, 12366, 12367]
        for gno in gno_list:
            result = evaluating_thresholding(Path('e:/src/Pdstats/data/{}_20230601000000_20240531000000.pickle'.format(gno)),
                                             [120, 240, 480, 600], [30, 300])
            for comb in result:
                print(comb[:8], len(comb)-8)
