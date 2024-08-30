import unittest
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator


def est_alarm_evaluating(count_int: int, count_others: int):
    return (2-(1.7*(0.9**(count_int*2))+0.3*(0.9**(count_others))))*0.5


def est_rule_trigger_evaluating(count_int: int, count_others: int):
    return (2-((0.9**(count_int))+(0.9**(count_others))))*0.5


class TestPDHealthyEvaluation(unittest.TestCase):
    def test_discharging_count_drawing(self):
        df = pd.DataFrame(np.fromfunction(est_alarm_evaluating, (20, 100)))
        x = df.columns
        y = df.index
        X, Y = np.meshgrid(x, y)
        Z = df
        fig, ax = plt.subplots(subplot_kw={"projection": "3d"})
        surf = ax.plot_surface(X, Y, Z, linewidth=0, antialiased=True, cmap=cm.jet)
        ax.set_xlim(0, 101)
        ax.set_ylim(0, 21)

        plt.xticks(np.arange(0, 100, 20))
        plt.yticks(np.arange(0, 20, 5))
        plt.xlabel('外部與電暈放電次數', fontdict=dict(family='Noto Sans TC'))
        plt.ylabel('內部放電次數', fontdict=dict(family='Noto Sans TC'))
        ax.set_zticks(np.arange(0, 1.1, 0.2))
        ax.set_zlabel('項次權重值', fontdict=dict(family='Noto Sans TC'))
        # fig.colorbar(surf, shrink=0.5, aspect=5)

        ax.mouse_init(zoom_btn=None)

        plt.show()

    def test_automatic_inspecting_drawing(self):
        df = pd.DataFrame(np.fromfunction(est_rule_trigger_evaluating, (20, 20)))
        x = df.columns
        y = df.index
        X, Y = np.meshgrid(x, y)
        Z = df
        fig, ax = plt.subplots(subplot_kw={"projection": "3d"})
        surf = ax.plot_surface(X, Y, Z, linewidth=0, antialiased=True, cmap=cm.jet)
        ax.set_xlim(0, 21)
        ax.set_ylim(0, 21)

        plt.xticks(np.arange(0, 21, 5))
        plt.yticks(np.arange(0, 21, 5))
        plt.xlabel('Rule_1觸發次數', fontdict=dict(family='Noto Sans TC'))
        plt.ylabel('Rule_2觸發次數', fontdict=dict(family='Noto Sans TC'))
        ax.set_zticks(np.arange(0, 1.1, 0.2))
        ax.set_zlabel('項次權重值', fontdict=dict(family='Noto Sans TC'))
        # fig.colorbar(surf, shrink=0.5, aspect=5)

        ax.mouse_init(zoom_btn=None)

        plt.show()
