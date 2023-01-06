SHELL := bash

.PHONY: black isort lint typecheck check-licenses fix-licenses unit-tests

lint:
	flake8

black:
	black .

isort:
	isort .

unit-tests:
	pytest -vv tests/unit/

typecheck:
	find . -type f -name "*.py" | grep -v ".git" | xargs mypy --config-file .mypy.ini

# Copyright Notices are handled by the next two targets
# See .licenserc.yaml for configuration
fix-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header fix

check-licenses:
	docker run  --rm -v $(CURDIR):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye:785bb7f3810572d6912666b4f64bad28e4360799 -v info -c .licenserc.yaml header check
