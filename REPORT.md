# Guided Midterm Report

## Student

* Name: Marios Kapetanos
* Date: 14/06/2026

## Part A - Docker Environment

Explain each service in 1-2 sentences:

* Kafka:
  Apache Kafka is used as a message broker to stream ride events from the producer application to the Spark streaming consumer in real time.

* Spark:
  Apache Spark is used for both batch and streaming data processing. It analyzes the ride dataset and processes live ride events received from Kafka.

## Part B - Dataset

* Number of rows:
  5000 ride records generated and stored in data/rides.csv.
  
* Important columns:
  ride_id, bike_id, station_start, station_end, started_at, ended_at, user_type

* Time columns:
  started_at, ended_at

## Part C - Batch Results

Paste or summarize your three outputs:

1. Rides per day:
   The dataset was grouped by ride date and the number of rides was calculated for each day.

2. Average duration by user type:
   The average ride duration was calculated separately for each user type.

3. Top start stations:
   The stations with the highest number of ride departures were identified. The most popular stations included Piraeus, Omonia, Monastiraki, Kifisia, Syntagma, Panepistimio, Gazi and Akropoli.

## Part D - Kafka Producer

* What topic did you publish to?
  rides-live

* What key did you use?
  ride_id

* Why is a key useful in Kafka?
  A Kafka key helps determine the partition where a message is stored. Using the same key preserves message ordering and improves data organization.

## Part E - Streaming Consumer

* What window size did you use?
  5 minutes

* What watermark did you use?
  10 minutes

* Where is your checkpoint folder?
  output/checkpoints/rides-live

## Part F - Short Answers

1. What is batch processing?
   Batch processing analyzes a stored dataset as a complete collection of records.

2. What is streaming?
   Streaming processes data continuously as new events arrive in real time.

3. What does Kafka do in this assignment?
   Kafka transfers ride events from the producer application to Spark Streaming for real-time analysis.

4. What is a watermark?
   A watermark defines how long Spark waits for late-arriving events before finalizing windowed aggregation results.

5. Why do we need checkpointing?
   Checkpointing stores streaming progress and state information so that processing can recover after failures without losing data.

6. What was the hardest part?
   The most challenging part was configuring Docker, Kafka and Spark to communicate correctly and ensuring that the streaming pipeline processed events successfully.
