#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 该文件用于 pip install -e . 将该项目安装为editable, 由于该命令需要使用setup.py， 但setup.py无法直接使用。
# 故在使用时需先将原setup.py删除，然后将本文件重命名为setup.py。
# 然后执行完 pip install -e . 之后用git checkout -- setup.py 将原来的setup.py恢复
# 这种方式可以避免设置hikyuu根目录到PYTHONPATH。

from setuptools import setup, find_packages
import os

hku_name = None
hku_ver = None

with open('xmake.lua', 'r') as f:
    for ln in f.readlines():
        if ln.strip().startswith('set_project'):
            hku_name = ln.split('"')[1]
        elif ln.strip().startswith('set_version'):
            hku_ver = ln.split('"')[1]
        if hku_name is not None and hku_ver is not None:
            break

if hku_name is None:
    hku_name = os.path.split(os.path.dirname(__file__))[1]
if hku_ver is None:
    hku_ver = '0.0.1'

setup(
    name=hku_name,
    version=hku_ver,
    packages=find_packages(),
    install_requires=[],
)
