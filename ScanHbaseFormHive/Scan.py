import time
import sys, getopt

'''
1、将所有需要对比的Hive数据(.txt)导出到本地
2、写脚本对其拼接成Hbase形式
3、从Hbse获取数据并和拼接数据进行对比
'''


class ScanCode(object):
    def __init__(self):
        # 配置文件路径
        self.configFilePath = ''
        # 存储配置文件数据 key:Hbase名 value:Hbase信息
        self.configInfo = dict()
        # 存储配置文件信息 key:hive表名 value:d对应的Hbase
        self.allConfigData = dict()
        # Hive数据文件路径
        self.HiveDataPath = ''
        # 存储全部Hive表名
        self.allHiveName = list()
        # 输出文件路径
        self.outPutPath = ''
        # 存储Hive数据
        self.Hive_data = list()
        # 存储拼接成的Hbase数据
        self.Hbase_mad = list()
        # 存储从Hbase上获取的数据
        self.Hbase_original = list()

    # 获取参数（配置文件、本地Hive数据路径、输出路径）
    def GetParam(self, argv):
        try:
            opts, args = getopt.getopt(argv, "hi:o:", ["configFilePath=", "HiveDataPath=", "outPutPath="])
        except getopt.GetoptError:
            print ("filename.py -c <configFilePath> -i <HiveDataPath> -o <outPutPath>")
            sys.exit(2)
        for opt, arg in opts:
            if opt == "-h":
                print ("filename.py -c <configFilePath> -i <HiveDataPath> -o <outPutPath>")
                sys.exit(2)
            elif opt in ("-c", "--conf"):
                self.configFilePath = arg
            elif opt in ("-i", "--ifile"):
                self.HiveDataPath = arg
            elif opt in ("-o", "--ofile"):
                self.outPutPath = arg

    # 获取全部配置文件信息
    def LoadConfigFile(self):
        with open(self.configFilePath, 'r') as LCF:
            for line in LCF.readlines():
                lines = line.strip().split('|')
                self.configInfo[lines[1]] = list(lines[1:])
                self.allConfigData[lines[0]] = lines[1]

    # 获取全部Hive数据名称
    def LoadHiveData(self):
        with open(self.HiveDataPath, 'r') as LHD:
            for line in LHD.readlines():
                self.allHiveName.append(line.strip())


if __name__ == '__main__':
    start = time.clock()
    demo = ScanCode()
    demo.GetParam(sys.argv[1:])
    end = time.clock()
    print ("消耗时间：%f s" % (end - start))
