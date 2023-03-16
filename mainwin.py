# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'pdstats.ui'
##
## Created by: Qt User Interface Compiler version 5.14.2
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide2.QtCore import (QCoreApplication, QDate, QDateTime, QMetaObject,
    QObject, QPoint, QRect, QSize, QTime, QUrl, Qt)
from PySide2.QtGui import (QBrush, QColor, QConicalGradient, QCursor, QFont,
    QFontDatabase, QIcon, QKeySequence, QLinearGradient, QPalette, QPainter,
    QPixmap, QRadialGradient)
from PySide2.QtWidgets import *


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(876, 440)
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(MainWindow.sizePolicy().hasHeightForWidth())
        MainWindow.setSizePolicy(sizePolicy)
        MainWindow.setMinimumSize(QSize(800, 420))
        font = QFont()
        font.setFamily(u"Noto Sans CJK TC Bold")
        font.setPointSize(12)
        font.setBold(True)
        font.setWeight(75)
        MainWindow.setFont(font)
        MainWindow.setAcceptDrops(True)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.centralwidget.setAcceptDrops(True)

        self.verticalLayout_2 = QVBoxLayout(self.centralwidget)
        self.verticalLayout_2.setObjectName(u"verticalLayout_2")

        self.verticalLayout = QVBoxLayout()
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.verticalLayout.setSizeConstraint(QLayout.SetMaximumSize)
        self.treeWidget = QTreeWidget(self.centralwidget)
        __qtreewidgetitem = QTreeWidgetItem()
        __qtreewidgetitem.setText(5, u"6")
        __qtreewidgetitem.setText(4, u"5")
        __qtreewidgetitem.setText(3, u"4")
        __qtreewidgetitem.setText(2, u"3")
        __qtreewidgetitem.setText(1, u"2")
        __qtreewidgetitem.setText(0, u"1")
        self.treeWidget.setHeaderItem(__qtreewidgetitem)
        self.treeWidget.setObjectName(u"treeWidget")
        self.treeWidget.setFont(font)
        self.treeWidget.setAcceptDrops(True)
        self.treeWidget.setDragDropOverwriteMode(True)
        self.treeWidget.setColumnCount(6)
        self.treeWidget.header().setVisible(False)

        self.verticalLayout.addWidget(self.treeWidget)

        self.headerlayout = QHBoxLayout()
        self.label1 = QLabel("Max Voltage")
        self.label1.setFont(font)

        self.label2 = QLabel("mv")
        self.label2.setFont(font)
        self.label2.setAlignment(Qt.AlignLeft)

        self.input_max_mv = QLineEdit()
        self.input_max_mv.setFont(font)
        self.input_max_mv.setAlignment(Qt.AlignRight)
        self.input_max_mv.setText("0")
        self.input_max_mv.setFixedWidth(64)

        self.label3 = QLabel("Lasting")
        self.label3.setFont(font)

        self.label4 = QLabel("min")
        self.label4.setFont(font)
        self.label4.setAlignment(Qt.AlignLeft)

        self.input_max_lasting_miniute = QLineEdit()
        self.input_max_lasting_miniute.setFont(font)
        self.input_max_lasting_miniute.setAlignment(Qt.AlignRight)
        self.input_max_lasting_miniute.setText("120")
        self.input_max_lasting_miniute.setFixedWidth(64)

        self.recalculate = QPushButton()
        self.recalculate.setText("Re-Calculate")
        self.recalculate.setFont(font)

        self.headerlayout.addStretch(1)
        self.headerlayout.addWidget(self.label1)
        self.headerlayout.addWidget(self.input_max_mv)
        self.headerlayout.addWidget(self.label2)

        self.headerlayout.addWidget(self.label3)
        self.headerlayout.addWidget(self.input_max_lasting_miniute)
        self.headerlayout.addWidget(self.label4)

        self.headerlayout.addWidget(self.recalculate)

        self.verticalLayout_2.addLayout(self.headerlayout)

        self.verticalLayout_2.addLayout(self.verticalLayout)

        MainWindow.setCentralWidget(self.centralwidget)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"pdstats", None))
    # retranslateUi

