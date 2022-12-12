# Makefile

# Run unit tests with pytest
.PHONY: test
test:
	pytest --capture=no src/test

# Format source
.PHONY: format
format:
	black --line-length 80 src/server/*.py
	black --line-length 80 src/server/backend/*.py

	black --line-length 80 src/test/*.py

# Sort imports
.PHONY: sort
sort:
	isort -e --line-length 80 src/server/*.py
	isort -e --line-length 80 src/server/backend/*.py

	isort -e --line-length 80 src/test/*.py

# Run quality assurance checks
.PHONY: qa
qa: sort format