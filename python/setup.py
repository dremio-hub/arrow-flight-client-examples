#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='arrow_flight_dremio',
      version='0.0.1',
      description='This package has shared components.',
      author='Adam Qin',
      author_email='adam.qin@vitesco.com',
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"])
    )
