#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/23
description:
"""
import pykafka
from pykafka import KafkaClient
client=KafkaClient(hosts='127.0.0.1:9092')
topics=client.topics
topic=client.topics['test']
print topics,topic
#异步消息
producer=topic.get_producer(delivery_reports=True)
producer.produce('test message')
