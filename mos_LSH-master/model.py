import torch
import numpy as np
from torch import nn
from torch.autograd import Variable
import torch.nn.functional as F


class MOS_LSTM(nn.Module):

    def __init__(self, input_size, hidden_size, output_size, batch_first=True, dropout=0, bidirectional=False):
        super(MOS_LSTM, self).__init__()
        self.hidden_size = hidden_size
        self.input_size = input_size
        self.num_layers = len(hidden_size)
        self.batch_first = batch_first
        self.Mlstm = []
        self.output_size = output_size
        print input_size,hidden_size,output_size
        for layer in range(self.num_layers):
            if layer == 0:
                self.Mlstm.append(nn.LSTM(input_size=input_size, hidden_size=self.hidden_size[layer],
                                          batch_first=batch_first).cuda())
            else:
                self.Mlstm.append(nn.LSTM(input_size=self.hidden_size[layer-1], hidden_size=self.hidden_size[layer],
                                          batch_first=batch_first).cuda())
        self.out_layer = nn.Linear(self.hidden_size[-1], 1)

    def forward(self, inputs):
        for layer in range(self.num_layers):
            if layer == 0:
                out, h = self.Mlstm[layer](inputs)
            else:
                out, h = self.Mlstm[layer](out)
        out = self.out_layer(out)
        out = F.sigmoid(out)
        return out




if __name__ == '__main__':
    x = np.ones((1, 48, 56), dtype=np.float32)
    x = torch.Tensor(x)
    x = Variable(x).cuda()
    model = MOS_LSTM(56, [64, 128], 1, batch_first=True).cuda()
    out = model(x)
    print out.size()
