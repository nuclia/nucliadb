# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from nucliadb_models import security


def test_security_groups_alias() -> None:
    assert (
        security.RequestSecurity(groups=["A", "B", "C"])
        == security.RequestSecurity(access_groups=["A", "B", "C"])  # type: ignore[call-arg]
        == security.RequestSecurity.model_validate({"groups": ["A", "B", "C"]})
        == security.RequestSecurity.model_validate({"access_groups": ["A", "B", "C"]})
    )
    assert security.RequestSecurity(groups=["A", "B", "C"]).groups == ["A", "B", "C"]
