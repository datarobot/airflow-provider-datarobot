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


.PHONY: clean
clean:  ## Clean up misc files like pytest and mypy cache for example
	rm -f dist/*
	rm -rf .mypy_cache
	find . -type f \( -name '*.py[co]' -o -name '*~' -o -name '@*' -o -name '#*#' -o -name '*.orig' -o -name '*.rej' -o -name '.chunk_*' -o -name 'tmp*' \) -delete
	find . -type d -name __pycache__ -delete
	rm -rf build/* && rm -rf airflow_provider_datarobot.egg-info
	rm -rf build/* && rm -rf airflow_provider_datarobot_early_access.egg-info

req: clean
	pip install --upgrade setuptools
	pip install 'pip<24.1'
	pip install -e .

req-dev: clean
	pip install --upgrade setuptools
	pip install 'pip<24.1'
	pip install -e ".[dev]"

req-dev-docs: clean
	pip install --upgrade setuptools
	pip install 'pip<24.1'
	pip install -e ".[dev,docs]"

lint:
	ruff check .

lint-fix:
	ruff check . --fix

format:
	ruff format .

format-no-fix:
	ruff format . --check

test-harness:
	pytest -vv tests/unit/ --junit-xml=unit_test_report.xml

unit-tests:
	pytest -vv tests/unit/

typecheck:
	mypy --config-file pyproject.toml datarobot_provider

test-docs:
	cd docs && $(MAKE) doctest

test-docs-harness:
	$(MAKE) test-docs

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
	grep -qF -- 'AIRFLOW__CORE__TEST_CONNECTION=Enabled' ./astro-dev/.env || \
	echo "AIRFLOW__CORE__TEST_CONNECTION=Enabled" >> ./astro-dev/.env

clean-astro-dev:
	-$(MAKE) kill-astro-dev
	rm -rf astro-dev
	$(MAKE) create-astro-dev

start-astro-dev:
	cd astro-dev && astro dev start --no-cache

stop-astro-dev:
	cd astro-dev && astro dev stop

kill-astro-dev:
	cd astro-dev && astro dev kill

build-astro-dev:
	-$(MAKE) stop-astro-dev
	rm -rf ./dist
	pip install --upgrade build
	python -m build
	cp -p "`ls -dtr1 ./dist/*.whl | sort -n | tail -1`" "./astro-dev/"
	echo "/usr/local/airflow/`find ./dist/*.whl -exec basename {} \; | sort -n | tail -1`" > \
 		./astro-dev/requirements_dev.txt
	cp -r ./datarobot_provider/example_dags/* ./astro-dev/dags/
	$(MAKE) start-astro-dev

copy-examples-astro-dev:
	cp -r ./datarobot_provider/example_dags/* ./astro-dev/dags/

autogen-operators:
	python ./datarobot_provider/autogen/generate_operator.py --whitelist ./datarobot_provider/autogen/whitelist.yaml --output_folder ./datarobot_provider/operators/gen

.PHONY: build-release
build-release: clean  ## Make release build
	pip3 install --no-cache-dir --upgrade pip setuptools wheel twine
	python setup.py sdist bdist_wheel

.PHONY: build-early-access
build-early-access: clean  ## Make early access build
	pip3 install --no-cache-dir --upgrade pip setuptools wheel twine
	python setup_early_access.py sdist bdist_wheel

test-development-container:
	$(MAKE) install-astro
	$(MAKE) clean-astro-dev
	$(MAKE) build-astro-dev
	chmod +x ./scripts/test_development_container.sh
	./scripts/test_development_container.sh
