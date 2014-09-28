import re
from setuptools import setup, find_packages
from pkg_resources import resource_string

# Regex groups: 0: URL part, 1: package name, 2: package version
EGG_RE = re.compile(r'^(.+)#egg=([a-z0-9_.]+)-([a-z0-9_.-]+)$')


def process_reqs(reqs):
    """
    filter out all egg-containint links from requirements file
    """
    pkg_reqs = []
    dep_links = []
    for req in reqs:
        egg_info = re.search(EGG_RE, req)
        if egg_info is None:
            pkg_reqs.append(req)
        else:
            _, egg = egg_info.group(1, 2)
            pkg_reqs.append(egg)
            dep_links.append(req)
    return pkg_reqs, dep_links

INSTALL_REQUIRES = process_reqs(
    resource_string(__name__, 'requirements.txt').splitlines())[0]
TESTS_REQUIRE = process_reqs(
    resource_string(__name__, 'requirements-tests.txt').splitlines())[0]

setup(
    name="mrdomino",
    version="0.0.10",
    author="James Knighton",
    author_email="knighton@livefyre.com",
    description=("Map-Reduce utility for DominoUp"),
    license="MIT",
    url='https://github.com/knighton/mapreduce',
    package_data={'mrdomino': ['*.sh']},
    packages=find_packages(exclude=['tests']),
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    zip_safe=True,
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2.7',
    ],
    long_description=resource_string(__name__, 'README.md'),
)
