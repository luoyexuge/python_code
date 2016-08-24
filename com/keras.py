#encoding:utf-8
'''
Created on 2016年7月6日

@author: wilson.zhou
'''

import pandas as pd
from keras.models import Sequential

#参数初始化
inputfile1 = '14362.csv'
data = pd.read_csv(inputfile1) 
x_train = data.iloc[:,:15].as_matrix().astype(int)
y_train = data.iloc[:,15].as_matrix().astype(int)
inputfile2 = '1080.csv'
data = pd.read_csv(inputfile2)
x_test = data.iloc[:,:15].as_matrix().astype(int)
y_test = data.iloc[:,15].as_matrix().astype(int)

from keras.layers.core import Dense, Activation

model = Sequential() #建立模型
model.add(Dense(15,14,))#15个输入层，14个隐含层
model.add(Activation('relu')) #用relu函数作为激活函数，能够大幅提供准确度
model.add(Dense(14,1))#14个隐含层，1个输出层
model.add(Activation('sigmoid')) #由于是0-1输出，用sigmoid函数作为激活函数

model.compile(loss = 'binary_crossentropy', optimizer = 'adam', class_mode = 'binary')
#编译模型。由于我们做的是二元分类，所以我们指定损失函数为binary_crossentropy，以及模式为binary
#另外常见的损失函数还有mean_squared_error、categorical_crossentropy等，请阅读帮助文件。
#求解方法我们指定用adam，还有sgd、rmsprop等可选

model.fit(x_train, y_train, nb_epoch = 100, batch_size = 1) #训练模型，学习100次
model.save_weights('net.model') #保存模型参数
yp = model.predict_classes(x_test).reshape(len(y_test)) #分类预测

def cm_plot(y_test, yp):
  
  from sklearn.metrics import confusion_matrix

  cm = confusion_matrix(y_test, yp)
  
  import matplotlib.pyplot as plt
  plt.matshow(cm, cmap=plt.cm.Greens)
  plt.colorbar()
  
  for x_test in range(len(cm)):
    for y_test in range(len(cm)):
      plt.annotate(cm[x_test,y_test], xy=(x_test, y_test), horizontalalignment='center', verticalalignment='center')
  
  plt.ylabel('True label')
  plt.xlabel('Predicted label')
  return plt

cm_plot(y_test,yp).show() #显示混淆矩阵可视化结果

outputfile='pre.csv'
pd.DataFrame(yp).to_csv(outputfile)