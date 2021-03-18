
#
# Copyright 2020 University of Southern California
# Distributed under the (new) BSD License. See LICENSE.txt for more info.
#

from distutils.core import setup

setup(
    name="bioformats_processing_client",
    description="Script for generating images for OSD",
    version="0.1-prerelease",
    scripts=[
        "bioformats_processing.py",
    ],
    requires=["bioformats_processing_lib"],
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
