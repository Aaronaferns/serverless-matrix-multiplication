# Serverless Matrix Multiplication

This project implements matrix multiplication using a serverless architecture. It leverages Google Cloud Functions, Pub/Sub, Redis, Memcache, and Google Cloud Storage to perform distributed and scalable matrix operations using the Map-Reduce paradigm.

## Features

- **Serverless Architecture**: Utilizes Google Cloud Functions for scalability and efficiency.
- **Map-Reduce Framework**: Implements the Map-Reduce paradigm for distributed matrix operations.
- **Intermediate Storage**: Stores intermediate results in Redis and Memcache for fast access.
- **Trigger-Based Workflow**: Workflow orchestrated using Pub/Sub and bucket triggers.

## Workflow

1. **Initialization**:
   - A file upload to the input bucket triggers the initialization function.
   - Matrices are loaded and split into chunks.

2. **Mapping**:
   - Mapper functions process matrix chunks and store intermediate results in Redis.
   - On completion, messages are published to the Mapper completion topic.

3. **Reducing**:
   - Reducer functions aggregate data for each matrix output element.
   - Results are stored in Memcache, and completion messages are published to the Reducer completion topic.

4. **Aggregation**:
   - Once all reducers complete, the aggregation function combines results into the final output matrix and stores it in the output bucket.

## Performance Metrics

| Matrix Size | Serverless Time | VM Time          |
|-------------|-----------------|------------------|
| 3x3         | 2 seconds       | 0.0 seconds      |
| 10x10       | 9 seconds       | 0.0 seconds      |
| 100x100     | 670 seconds     | 0.0012 seconds   |
| 1000x1000   | Timeout         | Not attempted    |

### Observations:
- Serverless architecture faces scalability challenges with larger matrices.
- Optimizations such as better chunking logic and mapper emissions could improve performance.

## Future Improvements

- Experiment with block matrix multiplication to optimize mapper logic.
- Refine chunking strategy for better load balancing.
- Investigate duplicate message handling in Pub/Sub to reduce processing delays.

## Repository Content

- **`init_function/`**: Code for the initialization function.
- **`mappers/`**: Mapper logic for processing matrix chunks.
- **`reducers/`**: Reducer logic for aggregating intermediate results.
- **`aggregation_function/`**: Code for creating the final output matrix.
- **`scripts/`**: Supporting scripts for deployment and testing.

## Requirements

- Google Cloud SDK
- Redis and Memcache instances
- Python 3.8+ (for the Cloud Functions)

## How to Run

1. **Setup Google Cloud Project**: Ensure you have access to Google Cloud Functions, Pub/Sub, and Storage.
2. **Deploy Functions**:
   - Deploy each function (init, mappers, reducers, aggregation) using the provided deployment scripts.
3. **Upload Matrices**: Upload matrix files to the designated input bucket.
4. **Monitor Execution**: Use Google Cloud Logging to track the progress of the workflow.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request with your suggestions and changes.
