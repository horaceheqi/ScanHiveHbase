import jpype
from jpype import *
import datetime

if __name__ == '__main__':
    # 直接调用该类
    # startJVM(getDefaultJVMPath())
    # JavaClass = JClass("JavaClass")
    # jc = JavaClass()
    # jc.setValue('111')
    # print(jc.getValue())
    # shutdownJVM()
    str = "funcode0001datetime20150112205012meridzhongzhichengtransidT98857count3"
    str2 = '201309'
    # 调用Jar包
    jvmPath = jpype.getDefaultJVMPath()
    # jarpath = 'MD5.jar'
    # jvmArg = '-Djava.class.path=%s' % jarpath
    # jpype.startJVM(jvmPath, '-ea', jvmArg)
    # javaClass = jpype.JClass("MD5.MD5Utils")
    # javaInstance = javaClass()
    # print(javaInstance.getMD5Str(str))
    jarpath = 'Month.jar'
    jvmArg = '-Djava.class.path=%s' % jarpath
    jpype.startJVM(jvmPath, '-ea', jvmArg)
    javaClass = jpype.JClass("Month.MonthUtil")
    javaInstance = javaClass()
    date = datetime.datetime.now()
    year = date.year
    month = date.month
    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1
    print(year, month)

    # print(javaInstance.month2int(date))
    jpype.shutdownJVM()
