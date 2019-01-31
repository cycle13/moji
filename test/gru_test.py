#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/11
description:GRU预测气温实例测试，
"""
import os,logging,sys,logging.handlers
import numpy as np
from matplotlib import pyplot as plt
from keras.models import Sequential
from keras import layers
from keras.optimizers import RMSprop
class gruTest_predict():
    def __init__(self,logger):
        self.logger=logger
    def predictTemprature(self):
        #加载数据
        data_dir = '/Users/yetao.lu/Downloads'
        fname = os.path.join(data_dir, 'jena_climate_2009_2016.csv')
        f = open(fname)
        data = f.read()
        f.close()
        
        lines = data.split('\n')
        header = lines[0].split(',')
        lines = lines[1:]
    
        print(header)
        print(len(lines))
        #把数据填充到数组格式
        float_data = np.zeros((len(lines), len(header) - 1))
        for i, line in enumerate(lines):
            values = [float(x) for x in line.split(',')[1:]]
            float_data[i, :] = values
        #print float_data
        temp = float_data[:, 1]
        #print temp
        #全部数据
        plt.plot(range(len(temp)), temp)
        #取前1440条记录
        plt.plot(range(1440), temp[:1440])
        #归一化
        mean = float_data[:200000].mean(axis=0)
        #print '前20万记录每列的均值：',mean
        float_data -= mean
        print '矩阵减均值：',float_data
        std = float_data[:200000].std(axis=0)
        print '标准差：',std
        float_data /= std
        print '归一化后的数据集：',float_data
        
        lookback = 1440
        step = 6
        delay = 144
        batch_size = 128
        train_gen = self.generator(float_data, lookback=lookback,
                              delay=delay, min_index=0,
                              max_index=200000, shuffle=True,
                              step=step, batch_size=batch_size)

        val_gen = self.generator(float_data, lookback=lookback,
                            delay=delay, min_index=200001,
                            max_index=300000,
                            step=step, batch_size=batch_size)

        test_gen = self.generator(float_data, lookback=lookback,
                             delay=delay, min_index=300001,
                             max_index=400000,
                             step=step, batch_size=batch_size)
        print  train_gen
        val_steps = (300000 - 200001 - lookback) // batch_size
        test_steps = (len(float_data) - 300001 - lookback) // batch_size
        self.evaluate_naive_method(val_steps,val_gen)

        #1.全连接网络
        # model = Sequential()
        # model.add(layers.Flatten(input_shape=(lookback // step,
        # float_data.shape[-1])))
        # model.add(layers.Dense(32, activation='relu'))
        # model.add(layers.Dense(1))
        #
        # model.compile(optimizer=RMSprop(), loss='mae')
        # history = model.fit_generator(train_gen, steps_per_epoch=500,
        #                               epochs=20,
        #                               validation_data=val_gen,
        #                               validation_steps=val_steps)
        # loss = history.history['loss']
        # val_loss = history.history['val_loss']
        # epochs = range(1, len(loss) + 1)
        # plt.figure()
        #
        # plt.plot(epochs, loss, 'bo', label='Training loss')
        # plt.plot(epochs, val_loss, 'b', label='Validation loss')
        # plt.title('Training and Validation loss')
        #
        # plt.show()
        #GRU
        model = Sequential()
        model.add(layers.GRU(32, input_shape=(None, float_data.shape[-1])))
        model.add(layers.Dense(1))

        model.compile(optimizer=RMSprop(), loss='mae')
        print val_steps
        history = model.fit_generator(train_gen, steps_per_epoch=500,
                                      epochs=3, validation_data=val_gen,
                                      validation_steps=val_steps)
        print history

        loss = history.history['loss']
        val_loss = history.history['val_loss']
        epochs = range(1, len(loss) + 1)
        plt.figure()

        plt.plot(epochs, loss, 'bo', label='Training loss')
        plt.plot(epochs, val_loss, 'b', label='Validation loss')
        plt.title('Training and Validation loss')

        plt.show()

        #
        # model = Sequential()
        # model.add(layers.GRU(32, dropout=0.2, recurrent_dropout=0.2,
        #                      input_shape=(None, float_data.shape[-1])))
        # model.add(layers.Dense(1))
        # model.compile(optimizer=RMSprop(), loss='mae')
        # history = model.fit_generator(train_gen, steps_per_epoch=500,
        #                               epochs=40, validation_data=val_gen,
        #                               validation_steps=val_steps)
    '''
    假设现在是1点，我们要预测2点时的气温，由于当前数据记录的是每隔10分钟时的气象数据，1点到2点
    间隔1小时，对应6个10分钟，这个6对应的就是delay

    要训练网络预测温度，就需要将气象数据与温度建立起对应关系，我们可以从1点开始倒推10天，从过去
    10天的气象数据中做抽样后，形成训练数据。由于气象数据是每10分钟记录一次，因此倒推10天就是从
    当前时刻开始回溯1440条数据，这个1440对应的就是lookback

    我们无需把全部1440条数据作为训练数据，而是从这些数据中抽样，每隔6条取一条，
    因此有1440/6=240条数据会作为训练数据，这就是代码中的lookback//step

    于是我就把1点前10天内的抽样数据作为训练数据，2点是的气温作为数据对应的正确答案，由此
    可以对网络进行训练
    '''

    def generator(self,data, lookback, delay, min_index, max_index,
                  shuffle=False,
                  batch_size=128, step=6):
        if max_index is None:
            max_index = len(data) - delay - 1
        i = min_index + lookback
        while 1:
            if shuffle:
                rows = np.random.randint(min_index + lookback, max_index,
                                         size=batch_size)
            else:
                if i + batch_size >= max_index:
                    i = min_index + lookback
                rows = np.arange(i, min(i + batch_size, max_index))
                i += len(rows)
            samples = np.zeros(
                (len(rows), lookback // step, data.shape[-1]))
            targets = np.zeros((len(rows),))
            for j, row in enumerate(rows):
                indices = range(rows[j] - lookback, rows[j], step)
                samples[j] = data[indices]
                targets[j] = data[rows[j] + delay][1]
            yield samples, targets

    def evaluate_naive_method(self,val_steps,val_gen):
        batch_maes = []
        for step in range(val_steps):
            samples, targets = next(val_gen)
            print samples,targets
            # preds是当前时刻温度，targets是下一小时温度
            preds = samples[:, -1, 1]
            print preds
            mae = np.mean(np.abs(preds - targets))
            batch_maes.append(mae)
        print(np.mean(batch_maes))
       
if __name__ == "__main__":
    # 日志模块
    logpath = '/Users/yetao.lu/Downloads/'
    logging.basicConfig()
    logger = logging.getLogger("logger")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile = os.path.join(logpath,'ele.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    hdlr = logging.handlers.TimedRotatingFileHandler(logfile, when='D',
                                                     interval=1,
                                                     backupCount=40)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    gru=gruTest_predict(logger)
    gru.predictTemprature()
