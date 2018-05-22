#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
:Description: PyExPool setup module

:Authors: (c) Artem Lutov <artem@exascale.info>
:Organizations: eXascale Infolab <http://exascale.info/>, Lumais <http://www.lumais.com/>
:Date: 2018-05
"""
from glob import glob  # Wildcards for files
from setuptools import setup

views = glob('views/*')
# print('>>> Views size:', str(len(views)), ', items:', views)
# images = glob('images/*')

setup(
	name='pyexpool',  # This is the name of your PyPI-package.
	# version='2.2.0',  # Update the version number for new releases
	version='3.0.0',  # Update the version number for new releases
	description=('A lightweight multi-process Execution Pool with load balancing'
	' and customizable resource consumption constraints.'),  # Required, "Summary" metadata field
	long_description=(
	'PyExPool is a concurrent execution pool with custom resource'
    ' constraints (memory, timeouts, affinity, CPU cores and caching) and load'
    ' balancing of the external applications on NUMA architecture.  '
	'All main functionality is implemented as a single-file module to be easily'
	' included into your project and customized as a part of your distribution '
	'(like in [PyCaBeM](https://github.com/eXascaleInfolab/PyCABeM)), not as a'
	' separate library. Additionally, an optional minimalistic Web interface is'
	' provided in the separate file to inspect the load balancer and execution pool.'
	' Typically, PyExPool is used as an application framework for benchmarking,'
	' load testing or other heavy-loaded multi-process execution activities on'
	' constrained computational resources.'
	'\n\n'
	'See details on the [PyExPool page](https://github.com/eXascaleInfolab/PyExPool)'
	' and star the project if you like it! For any further assistance you can drop me'
	' a email or write [me on Linkedin](https://linkedin.com/in/artemvl).'
	'\n\n'
	"""BibTeX:
```bibtex
@misc{pyexpool,
	author = {Artem Lutov and Philippe Cudré-Mauroux},
	title = {{PyExPool-v.3: A Lightweight Execution Pool with Constraint-aware Load-Balancer.}},
	year = {2018},
	url = {https://github.com/eXascaleInfolab/PyExPool}
}
```"""
	),
	long_description_content_type='text/markdown',
	url='https://github.com/eXascaleInfolab/PyExPool',
	author='Artem Lutov',
	author_email='artem@exascale.info',
	# Classifiers help users find your project by categorizing it.
	# For a list of valid classifiers, see https://pypi.org/classifiers/
	classifiers=[  # Optional
		# How mature is this project? Common values are
		#   3 - Alpha
		#   4 - Beta
		#   5 - Production/Stable
		#   6 - Mature
		'Development Status :: 4 - Beta',

		'Environment :: Plugins',
		'Environment :: Console',
		# 'Environment :: Web Environment',

		 # Indicate who your project is intended for
		'Intended Audience :: Developers',
		'Intended Audience :: Information Technology',
		'Intended Audience :: Science/Research',
		'Intended Audience :: System Administrators',

		# Pick your license as you wish
		'License :: OSI Approved :: Apache Software License',

		'Natural Language :: English',

		'Operating System :: POSIX',  # All featuresTopic :: Software Development :: Libraries :: Application Frameworks
		'Operating System :: Android',  # All features
		'Operating System :: Unix',  # Features might be limited
		'Operating System :: MacOS',  # Features might be limited
		'Operating System :: Microsoft',  # Limited Features
		'Operating System :: OS Independent',  # Limited Features

		# Specify the Python versions you support here. In particular, ensure
		# that you indicate whether you support Python 2, Python 3 or both.
		'Programming Language :: Python :: 2',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: 3',
		# 'Programming Language :: Python :: 3.2',
		# 'Programming Language :: Python :: 3.3',
		'Programming Language :: Python :: 3.4',
		'Programming Language :: Python :: 3.5',
		'Programming Language :: Python :: 3.6',
		'Programming Language :: Python :: 3.7',

		'Programming Language :: Python :: Implementation :: CPython',
		'Programming Language :: Python :: Implementation :: PyPy',

		'Topic :: System :: Benchmark',
		'Topic :: System :: Monitoring',
		'Topic :: Software Development :: Libraries :: Application Frameworks',
		'Topic :: Software Development :: Testing',
		'Topic :: Software Development :: Libraries :: Python Modules',
		'Topic :: Software Development',
		# 'Topic :: Scientific/Engineering :: Information Analysis',
		'Topic :: Scientific/Engineering',
		# 'Topic :: Office/Business',
		# 'Topic :: Utilities',
	],

	# This field adds keywords for your project which will appear on the
	# project page. What does your project relate to?
	#
	# Note that this is a string of words separated by whitespace, not a list.
	keywords=('execution-pool load-balancer task-queue multi-process benchmarking-framework'
		' execution-constraints NUMA concurrent parallel-computing cache-control monitoring-server'),  # Optional

	# You can just specify package directories manually here if your project is
	# simple. Or you can use find_packages().
	#
	# Alternatively, if you just want to distribute a single Python file, use
	# the `py_modules` argument instead as follows, which will expect a file
	# called `my_module.py` to exist:
	#
	# py_modules=["my_module"],
	# packages=find_packages(exclude=['contrib', 'docs', 'tests']),  # Required
	# packages=['pyexpool'],
	# packages=setuptools.find_packages(),
	# packages=['__init__'],
	py_modules=['mpepool', 'mpewui'],

	# This field lists other packages that your project depends on to run.
	# Any package you put here will be installed by pip when your project is
	# installed, so they must be valid existing projects.
	#
	# For an analysis of "install_requires" vs pip's requirements files see:
	# https://packaging.python.org/en/latest/requirements.html
	install_requires=['psutil>=5', 'bottle'
		# For Python2
		, 'future;python_version<"3"', 'enum34>=1;python_version<"3.4"'],  # Optional

	# List additional groups of dependencies here (e.g. development
	# dependencies). Users will be able to install these using the "extras"
	# syntax, for example:
	#
	#   $ pip install sampleproject[dev]
	#
	# Similar to `install_requires` above, these must be valid existing
	# projects.
	extras_require={  # Optional
		#'dev': ['check-manifest'],
		'test': ['mock>=2;python_version<"3"'],  # Only for Python 2
	},

	#package_dir={'pyexpool': '.'},

	# If there are data files included in your packages that need to be
	# installed, specify them here.
	#
	# If using Python 2.6 or earlier, then these have to be included in
	# MANIFEST.in as well.
	#
	# Include bottle template views and docs
	package_data={  # Optional
		# Note: images are relatively heavy, wildcards/regexp are not supported out of the box
		# '': ['views/restapi.htm'],  # Add README.md to the root and views list to the 'views'
		'': ['README.md'],  # Add README.md to the root and views list to the 'views'
		'views': views,  # Include views (bottle WebUI html templates)
		# 'views': ['webui.tpl'],  # Include views (bottle WebUI html templates)
		# 'images': images,  # Include images to the 'images'
	},
	# include_package_data=True,  # Deprecated

	# Although 'package_data' is the preferred approach, in some case you may
	# need to place data files outside of your packages. See:
	# http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files
	#
	# In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
	# data_files specifies a sequence of (directory, files) pairs in the following way
	# data_files=[
	# 	# ('', ['README.md']),
	# 	# ('images', glob("images/*.png")),
	# 	('views2', views),
	# ],  # Optional

	# To provide executable scripts, use entry points in preference to the
	# "scripts" keyword. Entry points provide cross-platform support and allow
	# `pip` to create the appropriate form of executable for the target
	# platform.
	#
	# For example, the following would provide a command called `sample` which
	# executes the function `main` from this package when invoked:
	# entry_points={  # Optional
	# 	'console_scripts': [
	# 		'sample=sample:main',
	# 	],
	# },

	# List additional URLs that are relevant to your project as a dict.
	#
	# This field corresponds to the "Project-URL" metadata fields:
	# https://packaging.python.org/specifications/core-metadata/#project-url-multiple-use
	#
	# Examples listed include a pattern for specifying where the package tracks
	# issues, where the source is hosted, where to say thanks to the package
	# maintainers, and where to support the project financially. The key is
	# what's used to render the link text on PyPI.
	project_urls={  # Optional
		'Bug Reports & Proposals': 'https://github.com/eXascaleInfolab/PyExPool/issues',
		# 'Funding': 'https://donate.pypi.org',
		# 'Say Thanks!': 'http://saythanks.io/to/example',
		'Source': 'https://github.com/eXascaleInfolab/PyExPool',
		'Visit our eXascale Infolab': 'https://exascale.info/'
	},
)
