# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# flake8: noqa

import itertools
import contextlib
import unittest
try:
    import numpy as np
except ImportError:
    # numpy is not firmly required
    np = None

import sys
import six
from six import BytesIO, StringIO, string_types as py_string


PY2 = sys.version_info[0] == 2


if PY2:
    import cPickle

    try:
        from cdecimal import Decimal
    except ImportError:
        from decimal import Decimal

    unicode_type = unicode
    lzip = zip
    zip = itertools.izip

    def dict_values(x):
        return x.values()

    range = xrange
    long = long

    def tobytes(o):
        if isinstance(o, unicode):
            return o.encode('utf8')
        else:
            return o

    def frombytes(o):
        return o

    def dict_iter(o):
        return o.items()

else:
    unicode_type = str
    def lzip(*x):
        return list(zip(*x))
    long = int
    zip = zip
    def dict_values(x):
        return list(x.values())
    from decimal import Decimal
    range = range

    def tobytes(o):
        if isinstance(o, str):
            return o.encode('utf8')
        else:
            return o

    def frombytes(o):
        return o.decode('utf8')

    def dict_iter(o):
        return list(o.items())


if sys.version_info < (3, 1):
    class CompatUnitTest(unittest.TestCase):
        @contextlib.contextmanager
        def assertRaisesRegex(self, expected_exception, expected_regex, *args, **kwargs):
            with self.assertRaisesRegexp(expected_exception, expected_regex, *args, **kwargs):
                yield
else:
    class CompatUnitTest(unittest.TestCase):
        pass

integer_types = six.integer_types
if np is not None:
    integer_types += (np.integer,)

# np.object and np.bool were deprecated in NumPy 1.20 and removed in 1.24.
# See: https://github.com/numpy/numpy/releases/tag/v1.24.0
# Use try/except so the aliases work on any installed NumPy version.
if np is not None:
    try:
        NP_OBJECT = np.object   # NumPy < 1.24
    except AttributeError:
        NP_OBJECT = object      # NumPy >= 1.24: use the plain Python builtin
    try:
        NP_BOOL = np.bool       # NumPy < 1.24
    except AttributeError:
        NP_BOOL = np.bool_      # NumPy >= 1.24: use the proper NumPy scalar
else:
    NP_OBJECT = object
    NP_BOOL   = bool
