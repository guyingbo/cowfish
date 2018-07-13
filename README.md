# cowfish

[![Build Status](https://travis-ci.org/guyingbo/cowfish.svg?branch=master)](https://travis-ci.org/guyingbo/cowfish)
[![Python Version](https://img.shields.io/pypi/pyversions/cowfish.svg)](https://pypi.python.org/pypi/cowfish)
[![Version](https://img.shields.io/pypi/v/cowfish.svg)](https://pypi.python.org/pypi/cowfish)
[![Format](https://img.shields.io/pypi/format/cowfish.svg)](https://pypi.python.org/pypi/cowfish)
[![License](https://img.shields.io/pypi/l/cowfish.svg)](https://pypi.python.org/pypi/cowfish)
[![codecov](https://codecov.io/gh/guyingbo/cowfish/branch/master/graph/badge.svg)](https://codecov.io/gh/guyingbo/cowfish)

A useful asynchronous library that is built on top of aiobotocore

## Usage

~~~
python -m cowfish.sqsprocesser queue_name region_name
~~~

## Examples

~~~python
firehose = Firehose(name, worker_params={'maxsize': 1000})

async def go():
    await firehose.put({'a': 3, 'b': 4})
    ...
    await firehose.stop()
~~~

## For dynamodb

use [aioboto3](https://github.com/terrycain/aioboto3) instead.
