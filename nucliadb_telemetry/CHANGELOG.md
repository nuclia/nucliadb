# CHANGELOG

## 1.1.8

- Improve error logging on errors while putting events on the queue

## 1.1.7

- Enable adding interceptors

## 1.1.6

- Open GRPCIO ping

## 1.1.5 (2022-09-14)

- Minimize sleep time

## 1.1.4 (2022-07-22)

- Make sure flush is done

## 1.1.3 (2022-07-20)

- Fix flushing of telemetry

## 1.1.2 (2022-07-15)

- Remove python 3.9 restriction

## 1.1.1 (2022-07-01)

- Fix: Do not create a span on nats messages without headers

## 1.1.0 (2022-07-01)

- Added support for jetstream pull subscriptions
- Added support for nats pub-sub and request-responses

## 1.0.7 (2022-06-15)

- Avoid telemetry on stream response right now

## 1.0.6 (2022-06-10)

- Fix jetstream helper functions asyncio

## 1.0.5 (2022-06-01)

- Simpler aproach on UDP sending packages

## 1.0.4 (2022-06-01)

- Fix await

## 1.0.3 (2022-06-01)

- Adding more debug information

## 1.0.2 (2022-05-30)

- Log in case event loop is dead

## 1.0.1 (2022-05-25)

- Fix jetstream wrapper

## 1.0.0 (2022-05-16)

- Initial release
