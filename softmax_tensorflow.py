import  tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data
import os
import sys
import logging
FILE = os.getcwd()
program=os.path.basename(sys.argv[0])
logger=logging.getLogger(program)
# logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s',datefmt='%Y %H:%M:%S')

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s:%(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename = os.path.join(FILE,'log.txt'),
                    filemode='w')
logging.root.setLevel(level=logging.INFO)
logger.info("running %s" % ' '.join(sys.argv))
mnist=input_data.read_data_sets("MNIST_data/", one_hot=True)
logger.info("download Done")
x=tf.placeholder(tf.float32,[None,784])

#paras
w=tf.Variable(tf.zeros([784,10]))
b=tf.Variable(tf.zeros([10]))
y=tf.nn.softmax(tf.matmul(x,w)+b)
y_=tf.placeholder(tf.float32,[None,10])

#loss func
cross_entropy=-tf.reduce_sum(y_*tf.log(y))
train_step= tf.train.GradientDescentOptimizer(0.01).minimize(cross_entropy)

#init
init= tf.initialize_all_variables()
sess=tf.Session()
sess.run(init)

#train
for i in xrange(10000):
    batch_xs,batch_ys=mnist.train.next_batch(100)
    sess.run(train_step,feed_dict={x:batch_xs,y_:batch_ys})
    if i%200==0:
        logger.info("cross_entropy is :"+str(sess.run(cross_entropy,feed_dict={x:batch_xs,y_:batch_ys})))

# correct_prediction是一个布尔值的列表，例如 [True, False, True, True]。
# 可以使用tf.cast()函数将其转换为[1, 0, 1, 1]，以方便准确率的计算
correct_prediction=tf.equal(tf.arg_max(y,1),tf.arg_max(y_,1))
accuarcy=tf.reduce_mean(tf.cast(correct_prediction,"float"))
logger.info("Accuarcy on Test-dataset: "+str(sess.run(accuarcy, feed_dict={x: mnist.test.images, y_: mnist.test.labels})))