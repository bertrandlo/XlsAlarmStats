# -*- coding: utf-8 -*-
import sys, pathlib
from queue import Queue
from mainwin import Ui_MainWindow
from PySide2.QtWidgets import QApplication, QMainWindow
from PySide2 import QtCore, QtGui, QtWidgets
from PySide2.QtCore import Signal, Slot
import qdarkstyle
from pdstats.data_import import DataImporter, DataSeries
from threading import Thread


class MainWindow(QMainWindow):
    signal_update = Signal(object)

    def __init__(self):
        super(MainWindow, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.centralwidget.setAcceptDrops(True)
        self.xls_filepath = None
        self.ds = None  # type: DataSeries
        self.job_queue = Queue()
        self.worker_thread = Thread(target=lambda: worker_thread(self.job_queue, self.signal_update), name="python_worker_thread")
        self.worker_thread.setDaemon(True)
        self.worker_thread.start()
        self.signal_update.connect(self.update_treewidget)
        self.root_item = None

    @QtCore.Slot(object)
    def update_treewidget(self, ds_report_list: dict):
        print("ds_report_list: {}".format(ds_report_list))
        root_item = QtWidgets.QTreeWidgetItem(self.ui.treeWidget, self.root_item)
        #root_item = QtWidgets.QTreeWidgetItem()

        for key, data in ds_report_list.items():
            item = QtWidgets.QTreeWidgetItem()
            item.setText(0, key)

            for i, value in data.items():
                info = QtWidgets.QTreeWidgetItem()
                info.setText(0, i+'x STD')
                info.setText(1, value[0])
                info.setText(2, value[1])
                info.setText(3, value[2])
                info.setText(4, value[3])
                info.setText(5, value[4])
                for col in range(info.columnCount()):
                    info.setTextAlignment(col, QtCore.Qt.AlignCenter)
                item.addChild(info)

            root_item.addChild(item)
        self.ui.treeWidget.insertTopLevelItem(0, root_item)
        self.ui.treeWidget.expandAll()
        # self.ui.treeWidget.resizeColumnToContents(0)

    def dragEnterEvent(self, event):
        event.acceptProposedAction()

    def dropEvent(self, event):
        url: QtCore.QUrl
        for url in event.mimeData().urls():
            file_extension = pathlib.PurePath(url.toLocalFile()).suffix
            print(file_extension)
            if file_extension != '.xls' and file_extension != '.xlsx':
                return
            root = self.ui.treeWidget.invisibleRootItem()
            for item in self.ui.treeWidget.selectedItems():
                (item.parent() or root).removeChild(item)

            self.xls_filepath = pathlib.Path(url.toLocalFile())

            self.root_item = QtWidgets.QTreeWidgetItem(self.ui.treeWidget, self.ui.treeWidget.headerItem())
            self.root_item.setText(0, self.xls_filepath.name)
            self.ui.treeWidget.setColumnWidth(0, 250)
            self.root_item.setText(1, "mean")
            self.ui.treeWidget.setColumnWidth(1, 250)
            self.root_item.setText(2, "std")
            self.ui.treeWidget.setColumnWidth(2, 100)
            self.root_item.setText(3, "threshold")
            self.ui.treeWidget.setColumnWidth(3, 120)
            self.root_item.setText(4, "Tmax")
            self.ui.treeWidget.setColumnWidth(4, 180)
            self.root_item.setText(5, "Alarm #")

            for col in range(self.root_item.columnCount()):
                self.root_item.setTextAlignment(col, QtCore.Qt.AlignCenter)

            self.job_queue.put_nowait(self.xls_filepath)


def worker_thread(job_queue: Queue, signal_update: Signal):
    while True:
        xls_filepath: pathlib.Path
        xls_filepath = job_queue.get(block=True)
        df = DataImporter.xls_import(xls_filepath)
        for idx, col in enumerate(df.columns):
            if col[0:7] != 'Unnamed':
                ds = DataSeries(df, idx, col)
                ds.report()
                signal_update.emit(ds.report_list)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet(qdarkstyle.load_stylesheet_pyside2())
    app.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyside2'))
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
