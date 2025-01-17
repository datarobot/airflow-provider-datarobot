SHELL := bash

# Determine the operating system
# "darwin" or "linux"
OS_NAME := $(shell uname -s | tr A-Z a-z)
ifeq ($(OS_NAME), darwin)
	ASTRO_CMD=brew install astro
else
	ASTRO_CMD=curl -sSL install.astronomer.io | sudo bash -s
endif

.PHONY: format format-no-fix lint lint-fix typecheck check-licenses fix-licenses unit-tests

req:
	pip install --upgrade pip setuptools
	pip install -e .

req-dev:
	pip install --upgrade pip setuptools
	pip install -e ".[dev]"

lint:
	ruff check .

lint-fix:
	ruff check . --fix

format:
	ruff format .

format-no-fix:
	ruff format . --check

unit-tests:
	pytest -vv tests/unit/

typecheck:
	find . -type f -name "*.py" | grep -v ".git" | xargs mypy --config-file .mypy.ini

test-docs:
	cd docs && $(MAKE) doctest

# Copyright Notices are handled by the next two targets
# See .licenserc.yaml for configuration
fix-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header fix

check-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header check

install-astro:
	$(ASTRO_CMD)

create-astro-dev:
	mkdir astro-dev
	cd astro-dev && astro dev init
	grep -qF -- 'RUN pip install -r \"/usr/local/airflow/requirements_dev.txt\"' ./astro-dev/Dockerfile || \
	echo "RUN pip install -r \"/usr/local/airflow/requirements_dev.txt\"" >> ./astro-dev/Dockerfile

clean-astro-dev:
	-$(MAKE) stop-astro-dev
	rm -rf astro-dev
	$(MAKE) create-astro-dev

start-astro-dev:
	cd astro-dev && astro dev start

stop-astro-dev:
	cd astro-dev && astro dev stop

build-astro-dev:
	-$(MAKE) stop-astro-dev
	rm -rf ./dist
	pip install --upgrade build
	python -m build
	cp -p "`ls -dtr1 ./dist/*.whl | sort -n | tail -1`" "./astro-dev/"
	echo "/usr/local/airflow/`find ./dist/*.whl -exec basename {} \; | sort -n | tail -1`" > \
 		./astro-dev/requirements_dev.txt
	$(MAKE) start-astro-dev

copy-examples-astro-dev:
	cp -r ./datarobot_provider/example_dags/* ./astro-dev/dags/
