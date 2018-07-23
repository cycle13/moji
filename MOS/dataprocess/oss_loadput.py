#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
aliyun oss2 utils
author:     meng.xiang
date:       2017/1/18
descirption: the utils lib to help data load from aliyun oss2
"""
import os
from itertools import islice
from time import time

import gevent
import oss2
from gevent import monkey
from gevent.pool import Pool
class OssUtils(object):
    def __init__(self, public_net=True, max_conn_pool=10):
        # default connection pool size
        # the pool size mast bg or eq than put&get thread number.
        """
        init function
        :param public_net: use public or aliyun private net.
        :param max_conn_pool: max num of threads
        """
        oss2.defaults.connection_pool_size = max_conn_pool
        self.is_public_net = public_net
        self.access_key_id = 'LTAIipB2qbeW3V99'
        self.access_key_secret = 'Dtyx9XKvFdQvmniSlns18j7lXpmA03'
        self.bucket_name = 'mojimeteo'
        if public_net:
            # public net like bj29 use this.
            # DO NOT USE THIS ADDRESS TO DOWNLOAD.
            self.endpoint = 'oss-cn-beijing.aliyuncs.com'
        else:
            # ali private net use this.
            self.endpoint = 'vpc100-oss-cn-beijing.aliyuncs.com'
        self.bucket = oss2.Bucket(
            oss2.Auth(self.access_key_id, self.access_key_secret),
            self.endpoint,
            self.bucket_name)
        self.greenlet_pool = Pool(max_conn_pool)
    
    def get_obj(self, key, dst, use_resume=True, part_size=(20 * 1024 * 1024),
                num_threads=4):
        """
        get files from oss,
        :param key: oss key
        :param dst: The path to save obj.
        :return obj save path at last
        """
        try:
            if self.is_public_net:
                raise Exception('Do not download from public')
            if use_resume:
                oss2.resumable_download(self.bucket, key, dst,
                                        store=oss2.ResumableDownloadStore(
                                            root='/tmp'),
                                        multiget_threshold=20 * 1024 * 1024,
                                        part_size=part_size,
                                        num_threads=num_threads)
            else:
                self.bucket.get_object_to_file(key, dst)
            return dst
        except Exception as ex:
            print ex.message
            return None
    
    def put_obj(self, key, src, use_resume=True, part_size=(20 * 1024 * 1024),
                num_threads=4):
        """
        put file to oss
        :param key:
        :param src:
        """
        # use resume
        try:
            if use_resume:
                oss2.resumable_upload(self.bucket, key, src,
                                      store=oss2.ResumableStore(root='/tmp'),
                                      multipart_threshold=100 * 1024,
                                      part_size=part_size,
                                      num_threads=num_threads)
            else:
                self.bucket.put_object_from_file(key, src)
        except Exception as ex:
            print ex.message
    
    def exists(self, key):
        """
        judge if the key exists
        :param key:
        :return:
        """
        return self.bucket.object_exists(key)
    
    def copy_obj(self, src_key, dst_key):
        """
        copy obj from src_key to dst_key
        :param key:
        :return:
        """
        return self.bucket.copy_object(self.bucket_name, src_key, dst_key)
    
    def list_obj(self, prefix='', delimiter='/', max_count=1000):
        """
        return a iterator in bucket
        :param prefix:
        :param max_count:
        :return:
        """
        return islice(oss2.ObjectIterator(self.bucket, delimiter=delimiter,
                                          prefix=prefix), max_count)
    
    def get_objs(self, src_root, dst_root):
        """
        get objs in oss
        """
        monkey.patch_socket()
        task_list = []
        if src_root.startswith('/'):
            src_root = src_root[1:]
        if not src_root.endswith('/'):
            src_root += '/'
        
        # make local dir if not exist
        def _touch_dir(path):
            result = False
            try:
                path = path.strip().rstrip("\\")
                if not os.path.exists(path):
                    os.makedirs(path)
                    result = True
                else:
                    result = True
            except:
                result = False
            return result
        
        _touch_dir(dst_root)
        
        def _get_objs(key):
            for obj in self.list_obj(key):
                # remove prefix of src_root
                _rel_path = obj.key[obj.key.index(src_root) + len(src_root):]
                local_obj = os.path.join(dst_root, _rel_path)
                if obj.is_prefix():  # directory
                    _touch_dir(local_obj)
                    _get_objs(obj.key)
                else:  # file
                    task_list.append((obj.key, local_obj, False))
                    self.get_obj(obj.key, local_obj, False)
                    print'file: %s->%s' % (obj.key, local_obj)
        
        _get_objs(src_root)
        tasks = self.greenlet_pool.imap(self.get_obj, task_list)
        tasks.join()
    
    def put_objs(self, src_root, dst_root):
        monkey.patch_all()
        task_list = []
        if dst_root.startswith('/'):
            dst_root = dst_root[1:]
        
        def _put_objs(_rel_path):
            local_path = os.path.join(src_root, _rel_path)
            filelist = os.listdir(local_path)
            for filename in filelist:
                oss_path = os.path.join(dst_root, _rel_path)
                key = os.path.join(oss_path, filename)
                local_obj = os.path.join(local_path, filename)
                _rel_path_obj = os.path.join(_rel_path, filename)
                if os.path.isdir(local_obj):
                    _put_objs(_rel_path_obj)
                else:
                    self.greenlet_pool.add(
                        gevent.spawn(self.put_obj, key, local_obj, False))
        
        _put_objs('')
        self.greenlet_pool.join()
if __name__ == '__main__':
    oss = OssUtils(False)
    # img_src = 'test'
    # s = timer()
    # OssUtils().put_obj(img_src, img_src, use_resume=False)
    # e = timer()
    # print '200MB上传单线程耗时%s' % (e - s)
    # s = timer()
    # OssUtils().put_obj(img_src, img_src)
    # e = timer()
    # print '200MB上传4线程耗时%s' % (e - s)
    # s = timer()
    # OssUtils().get_obj(img_src, img_src, use_resume=False)
    # e = timer()
    # print '200MB下载单线程耗时%s' % (e - s)
    # s = timer()
    # OssUtils().get_obj(img_src, img_src)
    # e = timer()
    # print '200MB下载4线程耗时%s' % (e - s)
    
    # for o in oss.list_obj(prefix=''):
    #     # if is_prefix is True,this obj is a dir
    #     print '%s:%s' % (o.key, o.is_prefix())
    
    ## put many obj to oss
    s = time()
    # oss.put_objs('./test', '/test/test')
    # oss.put_objs('moge/data/ecmwf/20180116/D1D01160000011603001.bz2', 'test')
    #oss.get_obj('moge/data/ecmwf/20180410/D1D04100000041003001.bz2','/home/wlan_dev/tmp/D1D04100000041003001.bz2')
    oss.get_objs('moge/data/ecmwf/20180116', '/mnt/data/ecdata')
    e = time()
    print '2000MB下载10协程耗时%s' % (e - s)
    ## get many obj from oss
    # oss.get_objs('test/test', './dir_test')
    #http: // mojimeteo.oss - cn - beijing.aliyuncs.com / moge / data / ecmwf / 20180410 / D1D04100000041003001.bz2