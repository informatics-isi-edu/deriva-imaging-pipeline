
#
# Copyright 2020 University of Southern California
# Distributed under the (new) BSD License. See LICENSE.txt for more info.
#

from distutils.core import setup

setup(
    name="deriva_imaging_processing",
    description="deriva-imaging-processing",
    version="0.1-prerelease",
    scripts=[
        "bin/deriva_imaging_processing_worker"
    ],
    requires=['os',
        'sys',
        'logging',
        'deriva'],
    maintainer_email="support@misd.isi.edu",
    license='(new) BSD',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3',
    ])
