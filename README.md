# AthensRide Data Engineering Pipeline

A streaming data engineering project developed as part of the **AUEB AI Data Factory – Machine Learning & Data Analysis Bootcamp**.

The project demonstrates the implementation of a real-time data processing pipeline using **Apache Kafka, Apache Spark Structured Streaming and Docker**.

## Project Overview

The goal of the project was to process ride data through a streaming architecture and generate useful analytics from incoming records.

The pipeline follows the general flow:

**Ride Data → Kafka → Spark Structured Streaming → Real-time Aggregations**

## Technologies Used

- Python
- Apache Kafka
- Apache Spark
- Spark Structured Streaming
- Docker
- Docker Compose

## Data Processing

The streaming pipeline processes ride data and produces analytics including:

- Number of rides per day
- Average ride duration by user type
- Most popular starting stations
- Time-window based streaming aggregations

Spark Structured Streaming was configured using **5-minute windows with a 10-minute watermark** for selected streaming operations.

## Architecture

Kafka is used as the event ingestion layer, while Spark Structured Streaming processes incoming ride events and performs real-time aggregations.

The project runs locally in a containerized environment using Docker Compose.

## Repository Structure

```text
athensride-data-engineering/
│
├── src/
│   ├── batch_analysis.py
│   ├── generate_rides.py
│   ├── producer.py
│   └── stream_analysis.py
│
├── data/
│   └── rides.csv
│
├── docker-compose.yml
├── requirements.txt
├── REPORT.md
├── README.md
└── .gitignore
```

## Key Learning Outcomes

Through this project I gained hands-on experience with:

- Building an end-to-end streaming data pipeline
- Using Kafka for event ingestion
- Processing streaming data with Spark Structured Streaming
- Applying time-window aggregations to streaming data
- Running a multi-service environment with Docker Compose
- Understanding how individual data engineering components interact within a complete pipeline

## Future Improvements

Possible extensions of the project include:

- Add automated data validation and error handling
- Introduce unit and integration testing
- Add monitoring and logging for pipeline health
- Build a dashboard for real-time analytics
- Deploy the pipeline to a cloud environment
- Improve scalability and fault tolerance
