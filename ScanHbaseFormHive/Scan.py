#encoding:utf-8
import time
import sys
import getopt
import happybase
import os
import re
import jpype
import datetime

'''
1、将所有需要对比的Hive数据(.txt)导出到本地
2、写脚本对其拼接成Hbase形式
3、从Hbse获取数据并和拼接数据进行对比
'''
# 测试数据
hiveDataPat_local = r'../HiveData/'
configFilePath_local = r'../Data/config.txt'
outPutPath = r'../outPutPath/out.txt'


class ScanCode(object):
    def __init__(self):
        # 配置文件路径
        self.configFilePath = configFilePath_local
        # 存储配置文件数据 key:Hbase名 value:Hbase信息
        self.configInfo = dict()
        # 存储配置文件信息 key:hbase表名 value:对应的Hive
        self.allConfigData = dict()
        # Hive数据文件路径
        self.HiveDataPath = hiveDataPat_local
        # 存储全部Hive表名
        self.allHiveName = list()
        # 存储全部Hbase表名
        self.allHbaseName = list()
        # 输出文件路径
        self.outPutPath = outPutPath
        # 存储Hive数据
        self.Hive_data = list()
        # 存储拼接成的Hbase数据
        self.Hbase_mad = list()
        # 存储从Hbase上获取的数据
        self.Hbase_original = list()
        # IP
        self.ip = ''
        # 存储时间
        self.time = ''

    # 获取参数（配置文件、本地Hive数据路径、输出路径、连接IP地址、时间）
    def GetParam(self, argv):
        try:
            opts, args = getopt.getopt(argv, "c:h:o:i:t:", ["configFilePath=", "HiveDataPath=", "outPutPath=", "IP", "time="])
        except getopt.GetoptError:
            print("filename.py -c <configFilePath> -h <HiveDataPath> -o <outPutPath> -i <IP> -t <time>")
            sys.exit(2)
        for opt, arg in opts:
            if opt == "-h":
                print("filename.py -c <configFilePath> -h <HiveDataPath> -o <outPutPath> -t <time>")
                sys.exit(2)
            elif opt in ("-c", "--conf"):
                self.configFilePath = arg
            elif opt in ("-h", "--hive"):
                self.HiveDataPath = arg
            elif opt in ("-o", "--out"):
                self.outPutPath = arg
            elif opt in ("-i", "--ip"):
                self.ip = opt
            elif opt in ("-t", "--time"):
                self.time = arg

    # 读取Hive、Hbase数据信息
    def LoadDataFile(self):
        print("读取config配置信息：")
        # 获取全部配置文件信息
        with open(self.configFilePath, 'r') as LCF:
            for line in LCF.readlines():
                lines = line.strip().split('|')
                self.configInfo[lines[1]] = list(lines[1:])
                self.allConfigData[lines[1]] = lines[0]
                # 读取全部需要对比的Hbase表名
                self.allHbaseName.append(lines[1])

        # 读取全部Hive数据(存储到本地.txt文件)
        for file in os.listdir(self.HiveDataPath):
            self.allHiveName.append(file.strip())

    # 读取Hive表中的详细信息并拼接处Hbase形式的串
    def LoadHiveDataDetial(self, Hbase, Hive):
        # Hive文档路径
        eachHivePath = self.HiveDataPath + Hive + '.txt'
        # Hive对应配置文档
        confDataInfo = self.configInfo[Hbase]
        if len(confDataInfo) == 6:
            tableName = confDataInfo[0]
            Des = confDataInfo[1]
            conlumn = confDataInfo[2]
            rowKeyDeli = confDataInfo[3]
            valueDeli = confDataInfo[4]
            flag = confDataInfo[5]
            with open(eachHivePath, 'r')as EHP:
                if flag == '1':
                    for line in EHP.readlines():
                        data_line = line.strip().split('\t')
                        roleKey = [data_line[data] for data in rowKeyDeli.split(',')]
                        roleKey.append("1")
                        value = [data_line[data] for data in valueDeli.split(',')]
                        yield roleKey
                        yield value
                elif flag == '2':
                    for line in EHP.readlines():
                        data_line = line.strip().split('\t')
                        roleKey = [data_line[data] for data in rowKeyDeli.split(',')]
                        roleKey.append("2")
                        value = [data_line[data] for data in valueDeli.split(',')]
                        yield roleKey
                        yield value
                elif flag == '3':
                    for line in EHP.readlines():
                        data_line = line.strip().split('\t')
                        roleKey = [data_line[data] for data in rowKeyDeli.split(',')]
                        value = [data_line[data] for data in valueDeli.split(',')]
                        yield roleKey
                        yield value
                elif flag == '4':
                    jvmPath = jpype.getDefaultJVMPath()
                    jarpath = 'Month.jar'
                    jvmArg = '-Djava.class.path=%s' % jarpath
                    jpype.startJVM(jvmPath, '-ea', jvmArg)
                    javaClass = jpype.JClass("Month.MonthUtil")
                    javaInstance = javaClass()
                    date = datetime.datetime.now().strftime('%Y%m')
                    for line in EHP.readlines():
                        data_line = line.strip().split('\t')
                        roleKey = [data_line[data] for data in rowKeyDeli.split(',')]
                        roleKey.append(javaInstance.month2int(self.time))
                        value = [data_line[data] for data in valueDeli.split(',')]
                        yield roleKey
                        yield value
                elif flag == '5':
                    jvmPath = jpype.getDefaultJVMPath()
                    jarpath = 'MD5.jar'
                    jvmArg = '-Djava.class.path=%s' % jarpath
                    jpype.startJVM(jvmPath, '-ea', jvmArg)
                    javaClass = jpype.JClass("MD5.MD5Utils")
                    javaInstance = javaClass()
                    for line in EHP.readlines():
                        data_line = line.strip().split('\t')
                        roleKey = [javaInstance.getMD5Str(data_line[int(data)]) for data in rowKeyDeli.split(',')]
                        value = [Des + ':' + conlumn + ':' + data_line[int(data)] for data in valueDeli.split(',')]
                        yield roleKey
                        yield value
                elif flag == '6':
                    roleKey_value = dict()
                    for line in EHP.readlines():
                        data_line = line.strip().split('\t')
                        roleKey = [data_line[int(data)] for data in rowKeyDeli.split(',')]
                        value = [Des+':'+conlumn+':'+data_line[int(data)] for data in valueDeli.split(',')]
                        roleKey_value[str(roleKey)] = str(value)
                    yield roleKey_value
        else:
            print("配置文件ConfigData有误!")

    # 获取需要对比Hbase表中数据，同时进行数据一致性对比
    def GetHbaseData(self):
        print("需要对比Hbase数据集个数:%d" % len(self.allHbaseName))
        print(self.allHbaseName)
        for Hbase in self.allHbaseName:
            Hive = self.allConfigData[Hbase]

            # 临时存放错误结果
            tempError = list()
            # 保存一直数据条数
            count = 0

            if Hbase in self.allConfigData:
                # 临时存放Hbase表中数据
                tempHbase = dict()

                # 正则
                pattern_key = re.compile("\['(.*)'\]")
                pattern_key2 = re.compile(r"b'(.*)'")
                pattern_value_hbase = re.compile(r"b'(.*):+(.*)':.+b'(.*)'")
                pattern_value_hive = re.compile("'(.*):+(.*):+(.*)'")
                connection = happybase.Connection('10.10.67.48')
                table = connection.table(Hbase)
                for k, v in table.scan():
                    temp_k = pattern_key2.findall(str(k))[0]
                    temp_v = pattern_value_hbase.findall(str(v))[0]
                    tempHbase[temp_k] = temp_v
                print("%s:数据条数%d" % (Hbase, len(tempHbase)))
                # 获取hive中数据与Hbase中数据进行对比
                tempHiveEach = next(self.LoadHiveDataDetial(Hbase, Hive))
                for key, value in tempHiveEach.items():
                    key = pattern_key.findall(str(key))[0]
                    value = pattern_value_hive.findall(value)[0]
                    # print(key, value)
                    if key in tempHbase and value == tempHbase[key]:
                        count += 1
                    else:
                        print("不相等!")
                        tempError.append("%s:%s\t%s\t%s\n" % (Hive+'-'+Hbase, key, value, key))
            else:
                print("请检查配置文档和Hive路径中是否有与Hbase对应数据！")
            print("%s:匹配正确数据条数%d" % (Hbase, count))
            # 保存结果
            if count == 50:
                with open(self.outPutPath, 'w', encoding='utf-8') as OPP:
                    OPP.write("正确匹配数量：50")
            elif len(tempError) != 0:
                with open(self.outPutPath, 'w', encoding='utf-8')as OPP:
                        OPP.writelines(tempError)


if __name__ == '__main__':
    start = time.clock()
    demo = ScanCode()
    demo.GetParam(sys.argv[1:])
    demo.LoadDataFile()
    demo.GetHbaseData()
    end = time.clock()
    print("消耗时间：%f s" % (end - start))
