#!/usr/bin/env python

from distutils.core import setup

setup(name='HUB-Datatools',
      version='1.0.0',
      author='Alejandro Caravaca Puchades',
      author_email='acaravacapuchades@icloud.com',
      url='https://github.com/acpuchades/hub-datatools',
      description='A set of utilities to help with data analysis tasks',
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
      )
