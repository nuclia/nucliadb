.. Copyright (C) 2021 Bosutech XXI S.L.
..
.. nucliadb is offered under the AGPL v3.0 and as commercial software.
.. For commercial licensing, contact us at info@nuclia.com.
..
.. AGPL:
.. This program is free software: you can redistribute it and/or modify
.. it under the terms of the GNU Affero General Public License as
.. published by the Free Software Foundation, either version 3 of the
.. License, or (at your option) any later version.
..
.. This program is distributed in the hope that it will be useful,
.. but WITHOUT ANY WARRANTY; without even the implied warranty of
.. MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
.. GNU Affero General Public License for more details.
..
.. You should have received a copy of the GNU Affero General Public License
.. along with this program. If not, see <http://www.gnu.org/licenses/>.

CHANGELOG
=========

1.1.3 (2022-07-20)
------------------

- Fix flushing of telemetry


1.1.2 (2022-07-15)
------------------

- Remove python 3.9 restriction


1.1.1 (2022-07-01)
------------------

- Fix: Do not create a span on nats messages without headers


1.1.0 (2022-07-01)
------------------

- Added support for jetstream pull subscriptions
- Added support for nats pub-sub and request-responses


1.0.7 (2022-06-15)
------------------

- Avoid telemetry on stream response right now


1.0.6 (2022-06-10)
------------------

- Fix jetstream helper functions asyncio


1.0.5 (2022-06-01)
------------------

- Simpler aproach on UDP sending packages


1.0.4 (2022-06-01)
------------------

- Fix await


1.0.3 (2022-06-01)
------------------

- Adding more debug information


1.0.2 (2022-05-30)
------------------

- Log in case event loop is dead


1.0.1 (2022-05-25)
------------------

- Fix jetstream wrapper


1.0.0 (2022-05-16)
------------------

- Initial release
