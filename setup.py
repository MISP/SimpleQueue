#!/usr/bin/python
# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='simplequeue',
    version='0.1',
    author='Raphaël Vinot',
    author_email='raphael.vinot@circl.lu',
    maintainer='Raphaël Vinot',
    url='https://github.com/MISP/SimpleQueue',
    description='Multiprocessed queuing system.',
    packages=['simplequeue'],
    scripts=['bin/managment.py', 'bin/QueueIn.py', 'bin/QueueOut.py'],
    classifiers=[
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Information Technology',
        'Programming Language :: Python :: 3',
    ],
    install_requires=['redis']
)
