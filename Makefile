# ClimateWorks DB - common tasks.  `make help` lists them.
PYTHON ?= python
YEARS  ?= 2023

.PHONY: help setup ingest build docs dashboard clean

help:  ## Show this help
	@grep -E '^[a-z]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-10s %s\n", $$1, $$2}'

setup:  ## Install Python dependencies
	$(PYTHON) -m pip install -r requirements.txt

ingest:  ## Load metadata + daily data, e.g. make ingest YEARS="2000 2024"
	$(PYTHON) get_data.py $(YEARS)

build:  ## Run every dbt seed, model and test
	dbt build

docs:  ## Generate and open the dbt docs site (lineage graph + column docs)
	dbt docs generate && dbt docs serve

dashboard:  ## Start the Streamlit dashboard
	streamlit run dashboard.py

clean:  ## Remove dbt artifacts and downloaded files
	dbt clean
	rm -rf data/raw logs
