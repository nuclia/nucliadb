# Environment variables

Usage guidelines:
- namespace should be used that describes the usage and dependency
- same settings should be used in env vars, helm charts and CLI usage
- reuse like settings like http and grpc server host and port


Settings[to be implemented yet]:
- `ENVIRONMENT`
- `LOG_LEVEL`
- `LOGGER_LEVELS`
- `DEBUG`
- `HTTP_HOST`
- `HTTP_CORS_ORIGINS`
- `GRPC_HOST`
- `METRICS_HTTP_HOST`
- `SEARCH_TIMEOUT`: Time in seconds for which the search endpoints wait for the index node search request response before we timeout and return an HTTP error.
- `SEARCH_CACHE_REDIS_HOST`: Hostname of the redis instance used as cache by the search component.
- `SEARCH_CACHE_REDIS_PORT`: Port of the redis instance used as cache by the search component.