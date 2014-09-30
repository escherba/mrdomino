.PHONY: clean lint virtualenv upgrade test package dev run

PACKAGE = mrdomino
PYENV = . env/bin/activate;
PYTHON = $(PYENV) python
PYTHON_TIMED = $(PYENV) time python

run: dev
	mkdir -p out
	$(PYTHON) examples/example.py \
		--no_clean \
		data/2014-01-18.detail.10000

package: env
	$(PYTHON) setup.py bdist_egg
	$(PYTHON) setup.py sdist

test: dev
	$(PYENV) nosetests --with-doctest $(NOSEARGS)

dev: env requirements-tests.txt
	$(PYENV) pip install -e . -r requirements-tests.txt

clean:
	test -f env/bin/activate && $(PYTHON) setup.py clean
	find $(PACKAGE) -type f -name "*.pyc" -exec rm {} \;
	rm -rf tmp/* out/*
	rm -rf build dist

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
