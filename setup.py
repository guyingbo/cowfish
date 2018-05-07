try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import os.path
import re
VERSION_RE = re.compile(r'''__version__ = ['"]([-a-z0-9.]+)['"]''')
BASE_PATH = os.path.dirname(__file__)


with open(os.path.join(BASE_PATH, 'cowfish', '__init__.py')) as f:
    try:
        version = VERSION_RE.search(f.read()).group(1)
    except IndexError:
        raise RuntimeError('Unable to determine version.')


with open(os.path.join(BASE_PATH, 'README.md')) as readme:
    long_description = readme.read()


setup(
    name='cowfish',
    description='A useful asynchronous library bases on aiobotocore',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT',
    version=version,
    author='Yingbo Gu',
    author_email='tensiongyb@gmail.com',
    maintainer='Yingbo Gu',
    maintainer_email='tensiongyb@gmail.com',
    url='https://github.com/guyingbo/cowfish',
    packages=['cowfish'],
    python_requires='>=3.5',
    install_requires=[
        'aiobotocore>=0.6.0',
    ],
    entry_points={
        'console_scripts': [
            'sqsprocesser = cowfish.sqsprocesser:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)
