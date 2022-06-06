init:
	pip3 install -r requirements.txt

flake8:
	flake8 --ignore=E501,F401,E128,E402,E731,F821 algoconnect

test:
	python3 -m unittest discover -s tests/

install:
	python3 setup.py install

publish:
	pip3 install 'twine>=1.5.0'
	python3 setup.py sdist bdist_wheel
	twine upload dist/*
	rm -fr build dist .egg requests.egg-info
