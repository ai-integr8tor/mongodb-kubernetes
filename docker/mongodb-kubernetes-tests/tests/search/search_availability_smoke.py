"""Acceptance smoke: steady-state search availability (single-cluster RS).

Consumes the availability harness (`SearchAvailabilityBackgroundTester` +
`SearchConnectivityTool`) and the deployment mixins from `tests/common/search`.
Deploys an external-source replica set with the managed Envoy LB, seeds
sample_mflix + an index, then runs a background `$search` load and asserts no
outage under steady state (no disruption). Disruption scenarios extend this by
injecting a fault between starting the tester and taking the verdict.
"""

from __future__ import annotations

import pytest
from kubetester.mongodb import MongoDB
from tests import test_logger
from tests.common.search.background_availability_tester import (
    SearchAvailabilityBackgroundTester,
    assert_no_outage,
)
from tests.common.search.bootstrap_test_mixins import (
    InstallOperatorTests,
    MongoDBRsDeploymentConfig,
    MongoDBRsDeploymentTests,
    SearchDeploymentTests,
    SearchE2EFixtures,
    SearchSampleDataAndIndexTests,
)
from tests.common.search.connectivity import SearchConnectivityTool

logger = test_logger.get_test_logger(__name__)

pytestmark = pytest.mark.e2e_search_availability_smoke

# paging-mode operations to observe across the steady-state window
STEADY_STATE_OPERATIONS = 30


def build_rs_config() -> MongoDBRsDeploymentConfig:
    # user names auto-derive from mdb_resource_name in __post_init__
    return MongoDBRsDeploymentConfig(mdb_resource_name="mdb-rs-avail")


class TestInstallOperator(InstallOperatorTests):
    pass


class TestSearchWithReplicaSet(SearchDeploymentTests, MongoDBRsDeploymentTests):
    def build_mongodb_rs_config(self) -> MongoDBRsDeploymentConfig:
        return build_rs_config()


class TestSearchSampleDataAndIndex(SearchSampleDataAndIndexTests, SearchE2EFixtures):
    def build_mongodb_rs_config(self) -> MongoDBRsDeploymentConfig:
        return build_rs_config()


class TestSteadyStateAvailability(SearchE2EFixtures):
    def build_mongodb_rs_config(self) -> MongoDBRsDeploymentConfig:
        return build_rs_config()

    def test_steady_state_availability(self, mdb: MongoDB):
        tool = SearchConnectivityTool(self._user_tester(mdb))
        with SearchAvailabilityBackgroundTester(tool, mode="paging") as bg:
            bg.wait_for_operations(STEADY_STATE_OPERATIONS)
        assert_no_outage(bg.verdict)
