# Data Extraction, Transformation, and Loading

This project focuses on the extraction, transformation, and loading (ETL) of data from a data warehouse into both an Amazon S3 bucket and a Postgres database. The ETL process is orchestrated using Airflow task groups, providing a scalable and reliable solution for data integration and management. The entire solution is containerized using Docker for easy deployment and management.

### Features

* Data Extraction: The project extracts data from a data warehouse using efficient query mechanisms to retrieve the required datasets for further processing. PostgresHook is used to establish a connection with the Postgres database and fetch the data.
* Data Transformation: The extracted data is transformed using various techniques, such as data cleaning, filtering, and aggregation, to ensure data quality and consistency.
* Data Loading: The transformed data is loaded into an Amazon S3 bucket for cost-effective storage and easy access, as well as into a Postgres database for further analysis and reporting. S3Hook is used to establish a connection with Amazon S3 and handle the data loading.
* Airflow Task Groups: The ETL process is organized and managed using Airflow task groups, allowing for efficient workflow management and task dependencies.
* Docker Containerization: The entire solution is containerized using Docker, providing a consistent and isolated environment for easy deployment, scaling, and management.

### Usage

* 1 . Clone the repository: git clone https://github.com/NnamdiNgwu/data_etl_aws.git
* 2 . Install Docker on your system.
* 3 . Build the Docker image: docker build -t data-etl .
* 4 . Configure the necessary parameters, such as the connection details for the data warehouse, Amazon S3, and Postgres database, within the Docker environment.
* 5 . Launch the Docker container: docker run -d --name data-etl-container data-etl
* 6 . Access the container logs to monitor the ETL process: docker logs data-etl-container
* 7 . Verify that the extracted data is successfully transformed and loaded into both the Amazon S3 bucket and the Postgres database.
* 8 . Use the data in the Amazon S3 bucket for further analysis, data processing, or sharing with other AWS services.
* 9 . Query the Postgres database to perform analysis, generate reports, or integrate with other applications.

### Airflow DAG

Below is a screenshot of the Airflow DAG (Directed Acyclic Graph) representing the ETL workflow:

DAG Screenshot

### Requirements

* Docker
* Access to the data warehouse for extraction
* AWS account with necessary permissions for Amazon S3 and Postgres database access

### Contributions

Contributions to this project are welcome! If you have any suggestions, bug fixes, or additional features, feel free to open an issue or submit a pull request.

### License

This project is licensed under the GPL-3.0 License.

Contact

For any inquiries or feedback, please reach out to nnamdingwu@yandex.com.

