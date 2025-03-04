# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

# This is a test fixture which is useful outside nucliadb tests. In particular
# it is used for the testbed. Keeping it under src so it can be imported outside
def reset_config():
    from nucliadb.common.cluster import settings as cluster_settings
    from nucliadb.ingest import settings as ingest_settings
    from nucliadb.train import settings as train_settings
    from nucliadb.writer import settings as writer_settings
    from nucliadb_utils import settings as utils_settings
    from nucliadb_utils.cache import settings as cache_settings

    all_settings = [
        cluster_settings.settings,
        ingest_settings.settings,
        train_settings.settings,
        writer_settings.settings,
        cache_settings.settings,
        utils_settings.audit_settings,
        utils_settings.http_settings,
        utils_settings.indexing_settings,
        utils_settings.nuclia_settings,
        utils_settings.nucliadb_settings,
        utils_settings.storage_settings,
        utils_settings.transaction_settings,
    ]
    for settings in all_settings:
        defaults = type(settings)()
        for attr, _value in settings:
            default_value = getattr(defaults, attr)
            setattr(settings, attr, default_value)
