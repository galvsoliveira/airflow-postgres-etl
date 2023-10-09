#!/bin/bash

airflow connections import /sources/credentials.json
airflow variables set ENVIRONMENT_SCHEDULE True
