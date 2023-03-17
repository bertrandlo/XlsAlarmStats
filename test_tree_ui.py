import qdarkstyle
from PySide2.QtCore import QDir, QFileInfo, QFile, QCoreApplication
from PySide2.QtWidgets import QTreeView, QFileSystemModel, QAbstractItemView, QApplication
from future.moves import sys
from treeview_window import Ui_MainWindow

from pdstat_gui import MainWindow


class TreeWindow(MainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.centralwidget.setAcceptDrops(True)
        self.setWindowTitle(QCoreApplication.translate("MainWindow", u"Trend", None))

    def dropEvent(self, event):
        print("Hello!")


class Tree(QTreeView):
    def __init__(self):
        QTreeView.__init__(self)
        model = QFileSystemModel()
        model.setRootPath(QDir.currentPath())

        self.setModel(model)
        self.setRootIndex(model.index(QDir.currentPath()))
        model.setReadOnly(False)

        self.setSelectionMode(self.SingleSelection)
        self.setDragDropMode(QAbstractItemView.InternalMove)
        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)

    def dragEnterEvent(self, event):
        m = event.mimeData()
        if m.hasUrls():
            for url in m.urls():
                if url.isLocalFile():
                    event.accept()
                    return
        event.ignore()

    def dropEvent(self, event):
        if event.source():
            QTreeView.dropEvent(self, event)
        else:
            ix = self.indexAt(event.pos())
            if not self.model().isDir(ix):
                ix = ix.parent()
            pathDir = self.model().filePath(ix)
            m = event.mimeData()
            if m.hasUrls():
                urlLocals = [url for url in m.urls() if url.isLocalFile()]
                accepted = False
                for urlLocal in urlLocals:
                    path = urlLocal.toLocalFile()
                    info = QFileInfo(path)
                    n_path = QDir(pathDir).filePath(info.fileName())
                    o_path = info.absoluteFilePath()
                    if n_path == o_path:
                        continue
                    if info.isDir():
                        QDir().rename(o_path, n_path)
                    else:
                        qfile = QFile(o_path)
                        if QFile(n_path).exists():
                            n_path += "(copy)"
                        qfile.rename(n_path)
                    accepted = True
                if accepted:
                    event.acceptProposedAction()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet(qdarkstyle.load_stylesheet_pyside2())
    app.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyside2'))
    window = TreeWindow()
    window.show()
    sys.exit(app.exec_())

