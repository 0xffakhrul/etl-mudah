# ETL-Mudah

ETL-Mudah is a project designed to extract, transform, and load data efficiently. This guide provides instructions to set up and run the project locally using Docker and Docker Compose.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- [Docker](https://docs.docker.com/get-docker/): To containerize the application.
- [Docker Compose](https://docs.docker.com/compose/install/): To manage multi-container Docker applications.

## Getting Started

Follow the steps below to set up and run the ETL-Mudah project locally.

### 1. Clone the Repository

Clone the project repository to your local machine:

```bash
git clone https://github.com/0xffakhrul/etl-mudah.git
cd etl-mudah
```
### 2. Set Up Environment Variables

Create a .env file in the root directory of the project to define necessary environment variables. For example:

# .env file
```bash
DATABASE_URL=postgresql://user:password@db:5432/etl_db
```
Replace user, password, and other placeholders with your actual configuration details.

### 3. Build and Run the Containers

Use Docker Compose to build and start the containers:

```bash
docker-compose up --build
```
This command will build the Docker images and start the services defined in the docker-compose.yaml file.

### 4. Accessing the Application

Once the containers are up and running:

Airflow Web UI: Access the Airflow dashboard by navigating to http://localhost:8080 in your web browser.
Streamlit Application: Access the Streamlit interface by navigating to http://localhost:8501 in your web browser.

### 5. Running ETL Jobs

To trigger the ETL processes:

Log in to the Airflow web interface.
Enable and trigger the desired DAGs (Directed Acyclic Graphs) corresponding to your ETL workflows.




