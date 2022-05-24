#!/usr/bin/env python

from distutils.core import setup
from pathlib import Path

root_dir = Path(__file__).parent
long_description = root_dir.joinpath('README.md').read_text()

setup(name='hub-datatools',
      version='1.1.0',
      author='Alejandro Caravaca Puchades',
      author_email='acaravacapuchades@icloud.com',
      url='https://github.com/acpuchades/hub-datatools',
      description='A set of utilities to help with data analysis tasks',
      long_description=long_description,
      long_description_content_type='text/markdown',
      packages=[
          'hub_datatools',
      ],
      entry_points={
          'console_scripts': [
              'dt-import=hub_datatools.scripts.import:main',
              'dt-export=hub_datatools.scripts.export:main',
              'dt-search=hub_datatools.scripts.search:main',
          ],
      },
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Intended Audience :: Science/Research',
          'Intended Audience :: Healthcare Industry',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Programming Language :: Python :: 3.12',
          'Topic :: Scientific/Engineering',
          'Topic :: Scientific/Engineering :: Bio-Informatics',
          'Topic :: Scientific/Engineering :: Information Analysis',
          'Topic :: Scientific/Engineering :: Medical Science Apps.',
      ]
      )
