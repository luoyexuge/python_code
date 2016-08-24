#encoding:utf-8
'''
Created on 2015年9月15日
@author: ZHOUMEIXU204
'''

path=u'D:\\Users\\zhoumeixu204\\Desktop\\python语言机器学习\\机器学习实战代码   python\\机器学习实战代码\\machinelearninginaction\\Ch05\\'
#梯度上升求最大值，梯度下降求最小值
import numpy as np
import matplotlib.pyplot as plt
import pandas  as pd
from   ggplot import *
import  matplotlib.pyplot   as plt
def loadDataSet():
    dataMat=[];labelMat=[]
    fr=open(path+'testSet.txt')
    for line in fr.readlines():
        lineArr=line.strip().split()
        dataMat.append([1.0,float(lineArr[0]),float(lineArr[1])])
        labelMat.append(int(lineArr[2]))
    return  dataMat,labelMat
def  sigmoid(inx):
    return 1.0/(1+np.exp(-inx))
def   gradAscent(dataMatin,classLabels):
    dataMatrix=np.mat(dataMatin)
    labelMat=np.mat(classLabels).T
    m,n=np.shape(dataMatin)
    print("m的值是:{0}".format(m))
    print("n的值是:{0}".format(n))
    alpha=0.0001
    maxCycles=5000
    weights=np.ones((n,1))
    for k in range(maxCycles):
        h=sigmoid(dataMatrix*weights)
        error=(labelMat-h)
        weights=weights+alpha*dataMatrix.T*error
    return  weights

dataArr,labelMat=loadDataSet()
weights=gradAscent(dataArr, labelMat)
print("梯度生生的参数为:{0}".format(weights))


#利用matplotlib.pyplot 画出决策边界   画出数据集和logistics回归的最佳你和直线的函数

def  plotBestFit(wei):
    weights=wei.getA()   #getA()函数把矩阵变为array的格式
    dataMat,labelMat=loadDataSet()
    dataArr=np.array(dataMat)
    n=np.shape(dataArr)[0]
    xcord1=[];ycordl=[]
    xcord2=[];ycord2=[]
    for  i in range(n):
        if int(labelMat[i])==1:
            xcord1.append(dataArr[i,1]);ycordl.append(dataArr[i,2])
        else:
            xcord2.append(dataArr[i,1]);ycord2.append(dataArr[i,2])
    fig=plt.figure()
    ax=fig.add_subplot(111)
    ax.scatter(xcord1,ycordl,s=30,c='red',marker='s')
    ax.scatter(xcord2,ycord2,s=30,c='green')
    x=np.arange(-3,3,0.1)
    y=(-weights[0]-weights[1]*x)/weights[2]
    ax.plot(x,y)
    plt.xlabel('x1')
    plt.ylabel('x2')
    plt.show()
if __name__=='__main__':
       plotBestFit(weights)    


#注意随机梯度于梯度上升算法的区别



#随机梯度上升算法
#一次仅用一个样本点来更新回归系数，称为随机梯度上升的算法，一次处理 所有的数据
#随机梯度上升算法实现过程


def  stocGradAscent0(dataMatrix,classlabels):
    dataMatrix=np.array(dataMatrix)
    m,n=np.shape(dataMatrix)
    alpha=0.01
    weights=np.ones(n)
    for  i in range(m):
        h=sigmoid(sum(dataMatrix[i]*weights))
        error=classlabels[i]-h
        weights=weights+alpha*error*dataMatrix[i]
    weights=np.mat(weights)
    return weights.T
dataArr,labelMat=loadDataSet()
weights=stocGradAscent0(dataArr, labelMat)
print("随机梯度生生的参数为:{0}".format(weights))
plotBestFit(weights)

#利用随机
def  stocGradAscent1(dataMatrix,classlabels,numIter=150):
    dataMatrix=np.array(dataMatrix)
    m,n=np.shape(dataMatrix)
    weights_x0=[];weights_x1=[];weights_x2=[];weights_ij=[]
    weights=np.ones(n)
    for j in range(numIter):
        dataIndex=range(m)
        for  i in range(m):
            alpha=4/(1.0+j+i)+0.01
            randIndex=int(np.random.uniform(0,len(dataIndex)))   #随机生成一个0到len(dataIndex)一个数，用于随度梯度下降
            h=sigmoid(sum(dataMatrix[randIndex]*weights))
            error=classlabels[randIndex]-h
            weights=weights+alpha*error*dataMatrix[randIndex]
            del(dataIndex[randIndex])
            weights_x0.append(weights[0])
            weights_x1.append(weights[1])
            weights_x2.append(weights[2])
    
    weights_ij.extend(xrange(1,numIter*m+1))
    weights_dataframe=pd.DataFrame({'weights_x0':weights_x0,'weights_x1':weights_x1,'weights_x2':weights_x2,'weights_ij':weights_ij})
    
    weights=np.mat(weights)
    return weights.T,weights_dataframe

dataArr,labelMat=loadDataSet()
weights,weights_dataframe=stocGradAscent1(dataArr, labelMat)
 
print("改进的随机梯度生生的参数为:{0}".format(weights))
plotBestFit(weights)
#描述参数变化情况
# print(ggplot(weights_dataframe,aes(x='weights_ij',y='weights_x1'))+geom_point(aes(color='red'))+geom_line())
# print(ggplot(weights_dataframe,aes(x='weights_ij',y='weights_x0'))+geom_point(aes(color='red'))+geom_line())
# print(ggplot(weights_dataframe,aes(x='weights_ij',y='weights_x2'))+geom_point(aes(color='red'))+geom_line()) 
plt.figure()
plt.plot(weights_dataframe.weights_ij,weights_dataframe.weights_x1,label='weights_x1')
plt.plot(weights_dataframe.weights_ij,weights_dataframe.weights_x0,label='weights_x0')
plt.plot(weights_dataframe.weights_ij,weights_dataframe.weights_x2,label='weights_x2')
plt.legend(loc='upper right')
plt.xlabel(u"迭代次数", fontproperties='SimHei')
plt.ylabel(u"参数变化", fontproperties='SimHei')
plt.title(u"迭代次数显示", fontproperties='SimHei')
plt.show()




#从疝气病预测病马的死亡率

def  classifyVector(Inx,weights):
    prob=sigmoid(sum(Inx*weights))
    if  prob>0.5:
        return 1.0
    else:
        return 0.0
def  colicTest():
    frTrain=open(path+'horseColicTraining.txt')
    frTest=open(path+'horseColicTest.txt')
    trainingSet=[];trainLabels=[]
    for  line  in frTrain.readlines():
        currLine=line.strip().split('\t')
        lineArr=[]
        for  i  in range(21):
            lineArr.append(float(currLine[i]))
        trainingSet.append(lineArr)
        trainLabels.append(float(currLine[21]))
    trainWeights,weights_dataframe=stocGradAscent1(np.array(trainingSet), trainLabels,500)
    errorCount=0;numTestVec=0.0
    for line  in frTest.readlines():
        numTestVec+=1.0
        currLine=line.strip().split('\t')
        lineArr=[]
        for i in range(21):
            lineArr.append(float(currLine[i]))
        if int(classifyVector(np.array(lineArr), trainWeights))!=int(currLine[21]):
            errorCount+=1
        errorRate=(float(errorCount))/numTestVec
        print ("the  error rate of this test is :%f"%errorRate)
    return errorRate
def  multiTest():
    numTest=10; errorSum=0.0
    for k in range(numTest):
        errorSum+=colicTest()
    print "after %d iterations the average error rate is :%f"%(numTest,errorSum/float(numTest))

if  __name__=='__main__':
    multiTest()
