# Convenience targets. The package runs without installation by setting PYTHONPATH=src,
# but `make install` gives you the `fdp` CLI.

PYTHON ?= python
PIP := $(PYTHON) -m pip
export PYTHONPATH := src

.PHONY: install generate run example test ci clean

install:
	$(PIP) install -e ".[dev]"

generate:
	$(PYTHON) scripts/generate_sample_data.py

run: generate
	$(PYTHON) -m pipeline.cli run

example: run
	$(PYTHON) examples/research_example.py

test:
	$(PYTHON) -m pytest -q

ci: generate test

clean:
	rm -rf data/raw data/normalized data/warehouse data/quarantine artifacts
