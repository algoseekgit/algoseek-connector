init:
	pip3 install -r requirements.txt

flake8:
	flake8 --ignore=E123,E501,F401,E128,E402,E731,F821,N816 algoseek_connector

test:
	python3 -m unittest discover -s tests/

install:
	python3 setup.py install

publish:
	pip3 install twine>=1.5.0 wheel
	python3 setup.py sdist bdist_wheel
	twine upload --verbose dist/*
	rm -fr build dist .egg algoseek_connector.egg-info
