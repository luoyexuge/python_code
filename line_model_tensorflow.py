# -*- coding: utf-8 -*-
import tensorflow as tf
import numpy as np
x_data=np.float32(np.random.rand(2,100)) #�������
y_data=np.dot([0.1,0.2],x_data)+0.3

# ����һ������ģ��
b=tf.Variable(tf.zeros([1]))
w=tf.Variable(tf.random_uniform([1,2],-1.0,1.0))
y=tf.matmul(w,x_data)+b

# ��С������
loss=tf.reduce_mean(tf.square(y-y_data))
optimizer=tf.train.GradientDescentOptimizer(0.5)
train=optimizer.minimize(loss)

 #��ʼ������
init = tf.initialize_all_variables()
#����ͼ��
sess=tf.Session()
sess.run(init)
for  step in xrange(0,201):
    sess.run(train)
    if step%10==0:
        print step,sess.run(w),sess.run(b)
