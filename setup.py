from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='Chapman',
      version=version,
      description="Actor-based job queue",
      long_description="""\
""",
      classifiers=[],
      keywords='',
      author='Rick Copeland',
      author_email='rick@arborian.com',
      url='',
      license='',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      scripts=[
          'scripts/chapmand',
          'scripts/chapmans',
          'scripts/chapmanev',
          'scripts/chapmon',
          'scripts/chapman-init',
          'scripts/chapman-ping',
          'scripts/chapman-kill',
          'scripts/chapman-unlock',
      ],
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      [paste.app_factory]
      chapman = chapman.http:http_main
      """,
      )
