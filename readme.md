# Real-Time Data Processing Pipeline

## Project Overview
This project demonstrates a real-time data processing pipeline using Google Cloud Pub/Sub for message management, MySQL for relational data storage, Cassandra for NoSQL data storage, and Docker for containerization. The pipeline processes order and payment data, updates records in a Cassandra table, and handles errors via a Dead Letter Queue (DLQ).

## Tech Stack
- Google Cloud Pub/Sub
- MySQL
- Cassandra
- Docker
- Python

## Prerequisites
- Docker installed
- Google Cloud SDK configured
- MySQL database set up

## Steps to Run the Application

1. **Clone the Repository**
   ```bash
   git clone <repository_url>
   cd <repository_directory>

