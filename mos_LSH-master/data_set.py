# encoding: utf-8
import torch
from torch.utils.data import Dataset, DataLoader
import numpy as np
from torchvision import transforms


class MOS_Dataset(Dataset):

    def __init__(self, path_file):
        self.path_file = path_file
        self.data = np.load(path_file)
        # 晴雨判断
        print self.data.shape
        self.features = self.data[:, :, :-2]
        print self.features.shape
        self.labels = self.data[:, :, -1]
        print self.labels.shape
        self.labels = self.labels[..., np.newaxis]
        print self.labels.shape
        n, s, f = self.features.shape
        print 'nsf',n,s,f
        self.features = self.features.reshape(-1, f)
        print self.features.shape
        mean = np.mean(self.features, axis=0)
        std = np.std(self.features, axis=0)
        np.save('mean.npy', mean)
        np.save('std.npy', std)
        self.features = (self.features - mean) / std
        self.features = self.features.reshape((n, s, f))
        self.labels[self.labels >= 0.1] = 1
        
    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):

        return torch.Tensor(self.features[idx]), torch.Tensor(self.labels[idx])


if __name__ == '__main__':
    dataset = MOS_Dataset('./data.npy')
    data = DataLoader(dataset, batch_size=10, shuffle=True)
    for i in data:
        print i[0].size(), i[1].size()
