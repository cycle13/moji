#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/18
description:
"""
import tensorflow as tf
class RNN:
    def __init__(self, batchsize, length):
        self.batchsize = batchsize
        self.outputshape = length

    def _input_add_state(self, input, state, active_fn=tf.nn.tanh, name=None):
        inputshape = input.get_shape().as_list()
        with tf.variable_scope(name):
            u = tf.get_variable(name='U', initializer=tf.random_uniform((inputshape[-1], self.outputshape)))
            w = tf.get_variable(name='W', initializer=tf.random_uniform((self.outputshape, self.outputshape)))
            b = tf.get_variable(name='B', initializer=tf.random_uniform((inputshape[0], self.outputshape)))
            return active_fn(tf.matmul(input, w) + tf.matmul(state, u) + b)


class GRU(RNN):
    def __init__(self, batchsize, length):
        super().__init__(batchsize, length)
        self.hidden = tf.Variable(tf.zeros((self.batchsize, self.outputshape)), trainable=False)
        self.candidate = tf.Variable(tf.random_uniform((self.batchsize, self.outputshape)), trainable=False)

    def build(self, inputs, reuse=False):
        with tf.variable_scope('GRU', reuse=reuse):
            update = self._input_add_state(inputs, self.hidden, name='update')
            reset = self._input_add_state(inputs, self.hidden, name='reset')
            self.hidden = tf.multiply(1 - update, self.hidden) + \
                          tf.multiply(update,
                                      self._input_add_state(inputs, tf.multiply(reset, self.hidden), name='candidate'))
        return self.hidden

