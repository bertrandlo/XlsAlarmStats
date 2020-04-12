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

        for key in ds_report_list:
            print(str(ds_report_list[key][0]))
            item = QtWidgets.QTreeWidgetItem(self.ui.treeWidget, self.root_item)
            item.setText(0, key+'x STD')
            item.setText(1, ds_report_list[key][0])
            item.setText(2, ds_report_list[key][1])
            item.setText(3, ds_report_list[key][2])
            item.setText(4, ds_report_list[key][3])
            item.setText(5, ds_report_list[key][4])
            for col in range(item.columnCount()):
                item.setTextAlignment(col, QtCore.Qt.AlignCenter)
            self.root_item.addChild(item)

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
            self.ui.treeWidget.resizeColumnToContents(0)
            self.root_item.setText(1, "mean")
            self.ui.treeWidget.setColumnWidth(1, 100)
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
