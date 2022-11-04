from py2neo import Graph, Node

import json
from csv import DictReader

import sys
from PyQt5.QtWidgets import QApplication, QFileDialog, QDialog,  QMessageBox
from PyQt5 import QtCore
from PyQt5.QtCore import *
import Csv2Graph

# Csv文件路径
filepath = ''
# 数据库访问地址
ip = ''
# 访问数据库用户名
user = ''
# 访问数据库密码
psw = ''
# 是否删除原始数据
deleteOri = 0

class Runthread(QtCore.QThread):
    #  通过类成员对象定义信号对象
    _signal = pyqtSignal(str)

    def __init__(self):
        super(Runthread, self).__init__()

    def __del__(self):
        self.wait()
    # 读取Csv文件内容，并调用主要处理函数
    def run(self):
        global filepath,ip,user,psw
        filepath = filepath.replace('\\', '/')
        result = self.csv_to_dict(filepath)
        resultDic = json.loads(result)
        print(resultDic)
        try:
            self.dict_to_graph(resultDic, ip, user, psw)
        except Exception as e:
            print(e)
            self._signal.emit(str(404))


    # 将Csv文件内容用字典列表存储
    def csv_to_dict(self,filepath):
        try:
            with open(filepath, 'r', encoding='utf-8-sig') as read_obj:
                dict_reader = DictReader(read_obj)
                list_of_dict = list(dict_reader)
                result = json.dumps(list_of_dict, indent=2)
            return result
        except IOError as err:
            print("I/O error({0})".format(err))

    # 通过结点ID获取结点
    def findNodeById(self,id, graphDic) -> dict:
        for i in graphDic:
            if i ['_id'] == id:
                return i
            if i['_id'] == '':
                print('找不到id为' + id + '的结点')
                return

    # 遍历字典列表创建实体、属性以及关系
    def dict_to_graph(self, graphDic, ip, user, psw):
        # 连接数据库
        graph = Graph(ip, auth=(user, psw))
        # 若复选框被选中，则删除原有数据
        if deleteOri == 1:
            graph.delete_all()
        gd = graphDic
        total = len(gd)
        count = 0
        nodeNum = 0
        for i in gd:
            # 创建节点
            if i['_id'] != '':
                count = count + 1
                self._signal.emit(str(count / total * 100))
                nodeNum = nodeNum + 1
                if count % 100 == 0 or count == 1:
                    print("正在处理第" + str(count) + "个结点")
                labels = i['_labels'].lstrip(":").split(":")
                node = Node(labels[0], name=i['name'])
                if len(labels) > 1:
                    for j in labels:
                        node.add_label(j)
                for key,value in i.items():
                    if value != '' and key != '_id' and key != '_labels':
                        node[key] = value
                graph.merge(node, labels[0], 'name')
                if count % 100 == 0 or count == 1:
                    print("第"+ str(count) +"个结点创建完毕！")
            # 创建关系
            elif i['_id'] == '' :
                count = count + 1
                self._signal.emit(str(count / total * 100))
                if (count - nodeNum) % 100 == 0 or count - nodeNum == 1:
                    print("正在建立第" + str(count - nodeNum) + "条关系")
                start = i['_start']
                end = i['_end']
                type = i['_type']
                startNode = self.findNodeById(start, gd)
                endNode = self.findNodeById(end, gd)
                graph.run(
                    'MATCH (entity1:' + startNode['_labels'].lstrip(":") + '{name:\'' + startNode['name'] + '\'}) ,'+
                    ' (entity2:' + endNode['_labels'].lstrip(":") + '{name:\'' + endNode['name'] + '\'}) '+
                    'MERGE (entity1)-[:'+ type +']->(entity2)')
                if (count - nodeNum) % 100 == 0 or count - nodeNum == 1:
                    print("第" + str(count - nodeNum) + "条关系建立完毕！")


class MainCode(QDialog,Csv2Graph.Ui_Dialog):
    def __init__(self):
        QDialog.__init__(self)
        Csv2Graph.Ui_Dialog.__init__(self)
        self.setupUi(self)
        self.pushButton.clicked.connect(self.on_open)
        self.buttonBox.accepted.connect(self.start_login)
        self.progressBar.setValue(0)

    def start_login(self):
        global filepath, ip, user, psw, deleteOri
        filepath = self.lineEdit.text()
        ip = self.lineEdit_2.text()
        user = self.lineEdit_3.text()
        psw = self.lineEdit_4.text()
        if self.checkBox.isChecked():
            deleteOri = 1
        else:
            deleteOri = 0
        # 创建线程
        self.thread = Runthread()
        # 连接信号
        self.thread._signal.connect(self.call_backlog)  # 进程连接回传到GUI的事件
        # 开始线程
        self.thread.start()

    def call_backlog(self, msg):
        if 0 <= int(float(msg)) and int(float(msg)) <= 100:
            self.progressBar.setValue(int(float(msg)))  # 将线程的参数传入进度条
        if int(float(msg)) == 100:
            msg_box = QMessageBox(QMessageBox.Information, '信息', '已成功完成CSV文件的导入！')
            msg_box.exec_()
        if int(float(msg)) == 404:
            msg_box = QMessageBox(QMessageBox.Information, '信息', '导入失败！')
            msg_box.exec_()

    # 选择CSV文件并获取其路径
    def on_open(self):
        txtstr = ""
        FullFileName, _ = QFileDialog.getOpenFileName(self, '选择', r'./', 'CSV (*.csv)')
        self.lineEdit.setText(FullFileName)
        filepath = FullFileName


if __name__ == "__main__":
    app = QApplication(sys.argv)
    md = MainCode()
    md.show()
    sys.exit(app.exec_())
