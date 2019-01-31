# encoding: utf-8
import numpy as np
import torch
from torch.autograd import Variable
from torch import nn
from torch import optim
from model import MOS_LSTM
from data_set import MOS_Dataset
from torch.utils.data import DataLoader

batch_size = 128


if __name__ == '__main__':

    dataset = MOS_Dataset('./data.npy')
    #加载数据
    dataload = DataLoader(dataset=dataset, batch_size=batch_size, shuffle=True)
    #损失值
    criterion = nn.BCELoss()
    model = MOS_LSTM(56, [64, 128, 64], 1, True)
    optimizer = optim.Adam(model.parameters(), lr=0.0001)

    for epoch in range(100):

        for i, data in enumerate(dataload):
            inputs, labels = data
            inputs = Variable(inputs).cuda()
            labels = Variable(labels).cuda()
            optimizer.zero_grad()
            model = model.cuda()

            outputs = model(inputs).cuda()
            loss = criterion(outputs, labels)

            loss.backward()

            optimizer.step()

            if i % 100 == 0:
                print 'epoch %d step %d: loss= %f' % (epoch, i, loss)








