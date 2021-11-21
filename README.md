Java code for the continuous generation of random retail **invoices**, both semantically valid and invalid, into a `Kafka` topic. It facilitates development of **retail business-logic** `microservices`, as well as **data-quality** `microservices`. A downstream `Kafka Consumer` (not included in the code) serializes events to `Azure Data Lake Storage`, which begins `Azure Data Pipelines` that trigger the execution of parameterized (widgetized) `Databricks Notebooks` that contain `Machine Learning` logic in them. (See image below). The code works with `Standalone Kafka` as well as with `Confluent Kafka Platform`.

[See related PDF with Data Model here](https://jupyter.ai/cml)

![Continuous M/L Simulation On Azure](resources/images/CONTINUOUS.ML.with.ADF.and.DATABRICKS.png?raw=true "Continuous M/L Simulation On Azure"){:target="_blank"}

