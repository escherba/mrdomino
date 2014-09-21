.PHONY: clean lint virtualenv upgrade test package dev run

PACKAGE = mrdomino
PYENV = . env/bin/activate;
PYTHON = $(PYENV) python
PYTHON_TIMED = $(PYENV) time python

run: dev
	$(PYTHON) examples/example.py ./data/2014-01-18.detail.sorted.gz mrdomino/exec.sh

package: env
	$(PYTHON) setup.py bdist_egg
	$(PYTHON) setup.py sdist

test: env dev
	$(PYENV) nosetests --with-doctest $(NOSEARGS)

dev: env/bin/activate dev_requirements.txt
	$(PYENV) pip install -e . -r dev_requirements.txt

clean:
	test -f env/bin/activate && $(PYTHON) setup.py clean
	find $(PACKAGE) -type f -name "*.pyc" -exec rm {} \;
	rm -rf tmp/* out/*

lint: dev
	$(PYTHON) setup.py lint

nuke: clean
	rm -rf *.egg *.egg-info env bin cover coverage.xml nosetests.xml

env virtualenv: env/bin/activate
env/bin/activate: requirements.txt setup.py
	test -f $@ || virtualenv --no-site-packages env
	$(PYENV) pip install -e . -r requirements.txt
	touch $@

upgrade: env
	$(PYENV) pip install -e . -r requirements.txt --upgrade
