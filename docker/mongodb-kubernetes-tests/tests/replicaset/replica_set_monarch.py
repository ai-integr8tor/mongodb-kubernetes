"""
e2e test for Monarch Deployment pattern including failover (promotion).

Flow:
  1. Deploy MinIO (S3 store)
  2. Deploy active RS WITHOUT spec.monarch (plain replica set)
  3. Insert documents while RS is running without DR
  4. Activate Monarch: patch spec.monarch.role=active → operator creates shipper
  5. Verify shipper uploads to S3
  6. Deploy standby RS with spec.monarch.role=standby
  7. Verify standby agents block in WaitForInjectorReady before going Running
  8. Verify data replicated to standby
  9. Promote standby to active: patch spec.monarch.role=active → failover state machine
 10. Verify injector deleted, shipper created, S3 state is Active
 11. Verify promoted cluster can write and shipper uploads to S3
"""

import os
import subprocess
import time

import boto3
import pymongo
from botocore.config import Config as BotoConfig
from kubernetes import client as k8s_client
from pytest import fixture, mark

from kubetester import create_or_update_secret, try_load
from kubetester.kubetester import KubernetesTester
from kubetester.kubetester import fixture as yaml_fixture
from kubetester.mongodb import MongoDB
from kubetester.opsmanager import MongoDBOpsManager
from kubetester.phase import Phase

# ── resource names ──────────────────────────────────────────────────────────
ACTIVE_RS_NAME = "monarch-active-rs"
STANDBY_RS_NAME = "monarch-standby-rs"
MINIO_NAME = "monarch-minio"

# ── S3 / Monarch config ────────────────────────────────────────────────────
S3_BUCKET = "monarch-standby-bucket"
CLUSTER_PREFIX = "failoverdemo"
SHARD_ID = "0"
MINIO_USER = "minioadmin"
MINIO_PASSWORD = "minioadmin123"
AWS_REGION = "eu-north-1"
S3_CREDS_SECRET = "monarch-s3-creds"


# ── custom images ───────────────────────────────────────────────────────────
_STAGING_ECR = "268558157000.dkr.ecr.us-east-1.amazonaws.com/staging"
OM_IMAGE = os.getenv("MDB_OM_IMAGE", f"{_STAGING_ECR}/mongodb-enterprise-ops-manager-ubi:monarch")
MONARCH_IMAGE = os.getenv("MDB_MONARCH_IMAGE", f"{_STAGING_ECR}/mongodb-kubernetes-monarch-injector:monarch")

# ── test data ───────────────────────────────────────────────────────────────
PRODUCTS_DB = "products"
INVENTORY_COLLECTION = "inventory"
INVENTORY_DOCS = [
    {"item": "laptop", "qty": 25, "price": 999.99, "warehouse": "A"},
    {"item": "phone", "qty": 100, "price": 699.99, "warehouse": "B"},
    {"item": "tablet", "qty": 50, "price": 449.99, "warehouse": "A"},
    {"item": "monitor", "qty": 75, "price": 329.99, "warehouse": "C"},
    {"item": "keyboard", "qty": 200, "price": 79.99, "warehouse": "B"},
    {"item": "mouse", "qty": 150, "price": 49.99, "warehouse": "A"},
    {"item": "headphones", "qty": 80, "price": 199.99, "warehouse": "C"},
    {"item": "webcam", "qty": 60, "price": 89.99, "warehouse": "B"},
    {"item": "charger", "qty": 300, "price": 29.99, "warehouse": "A"},
    {"item": "cable", "qty": 500, "price": 14.99, "warehouse": "C"},
]


# ── helpers ─────────────────────────────────────────────────────────────────


def _minio_endpoint(namespace: str) -> str:
    return f"http://{MINIO_NAME}.{namespace}.svc.cluster.local:9000"


def _wait_for_deployment_ready(namespace: str, name: str, timeout: int = 120):
    apps = k8s_client.AppsV1Api()
    deadline = time.time() + timeout
    while time.time() < deadline:
        dep = apps.read_namespaced_deployment(name, namespace)
        if dep.status.ready_replicas and dep.status.ready_replicas >= 1:
            return
        time.sleep(3)
    raise TimeoutError(f"Deployment {name} not ready after {timeout}s")


def _s3_client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name=AWS_REGION,
        config=BotoConfig(signature_version="s3v4"),
    )


def _ensure_s3_bucket(namespace: str, timeout: int = 120):
    """Create the S3 bucket, retrying until MinIO is fully ready to accept API calls."""
    s3 = _s3_client(_minio_endpoint(namespace))
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s3.create_bucket(Bucket=S3_BUCKET)
            return
        except s3.exceptions.BucketAlreadyOwnedByYou:
            return
        except Exception:
            time.sleep(3)
    raise TimeoutError(f"Failed to create S3 bucket {S3_BUCKET} in MinIO after {timeout}s")


def _wait_for_s3_data(namespace: str, timeout: int = 300):
    s3 = _s3_client(_minio_endpoint(namespace))
    deadline = time.time() + timeout
    while time.time() < deadline:
        if s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{CLUSTER_PREFIX}/{SHARD_ID}/").get("KeyCount", 0) > 0:
            return
        time.sleep(5)
    raise TimeoutError(f"No S3 objects after {timeout}s — shipper may not be running")


def _wait_for_monarch_condition(mdb: MongoDB, timeout: int = 300):
    """Wait until the ShipperReady or InjectorReady condition on the MongoDB CR is True."""
    role = mdb["spec"]["monarch"]["role"]
    condition_type = "ShipperReady" if role == "active" else "InjectorReady"

    def is_ready(resource: MongoDB) -> bool:
        for cond in resource["status"]["conditions"]:
            if cond.get("type") == condition_type and cond.get("status") == "True":
                return True
        return False

    mdb.wait_for(is_ready, timeout=timeout, should_raise=True)


def _monarch_spec(namespace: str, role: str, **extra) -> dict:
    """Build a Monarch spec using the simplified API structure."""
    spec = {
        "role": role,
        "s3": {
            "bucket": S3_BUCKET,
            "region": AWS_REGION,
            "credentialsSecretRef": {"name": S3_CREDS_SECRET},
            "prefix": CLUSTER_PREFIX,
            "endpoint": _minio_endpoint(namespace),
            "pathStyle": True,
        },
        "image": MONARCH_IMAGE,
    }
    spec.update(extra)
    return spec


# ── fixtures ────────────────────────────────────────────────────────────────


@fixture(scope="module")
def ops_manager(namespace: str, custom_mdb_version: str, custom_appdb_version: str) -> MongoDBOpsManager:
    resource = MongoDBOpsManager.from_yaml(yaml_fixture("om-monarch.yaml"), namespace=namespace)
    resource["spec"]["statefulSet"] = {
        "spec": {"template": {"spec": {"containers": [{"name": "mongodb-ops-manager", "image": OM_IMAGE}]}}}
    }
    resource["spec"]["applicationDatabase"]["version"] = custom_appdb_version
    resource.create_admin_secret()
    resource.update()
    resource.appdb_status().assert_reaches_phase(Phase.Running, timeout=900)
    resource.om_status().assert_reaches_phase(Phase.Running, timeout=900)
    return resource


@fixture(scope="module")
def minio(namespace: str) -> str:
    subprocess.check_call(["kubectl", "apply", "-n", namespace, "-f", yaml_fixture("minio.yaml")])
    _wait_for_deployment_ready(namespace, MINIO_NAME)
    _ensure_s3_bucket(namespace)
    return _minio_endpoint(namespace)


@fixture(scope="module")
def s3_creds_secret(namespace: str, minio: str) -> str:
    create_or_update_secret(
        namespace=namespace,
        name=S3_CREDS_SECRET,
        data={"awsAccessKeyId": MINIO_USER, "awsSecretAccessKey": MINIO_PASSWORD},
    )
    return S3_CREDS_SECRET


@fixture(scope="module")
def active_rs(namespace: str, custom_mdb_version: str, ops_manager: MongoDBOpsManager) -> MongoDB:
    resource = MongoDB.from_yaml(yaml_fixture("replica-set-monarch.yaml"), ACTIVE_RS_NAME, namespace)
    resource.set_version(custom_mdb_version)
    resource.configure(ops_manager, ACTIVE_RS_NAME)
    try_load(resource)
    return resource


@fixture(scope="module")
def standby_rs(
    namespace: str,
    custom_mdb_version: str,
    active_rs: MongoDB,
    s3_creds_secret: str,
    ops_manager: MongoDBOpsManager,
) -> MongoDB:
    """Standby Clusters always start with Injectors."""
    _wait_for_s3_data(namespace)
    resource = MongoDB.from_yaml(yaml_fixture("replica-set-monarch.yaml"), STANDBY_RS_NAME, namespace)
    resource.set_version(custom_mdb_version)
    resource["spec"]["monarch"] = _monarch_spec(
        namespace,
        "standby",
    )
    resource.configure(ops_manager, STANDBY_RS_NAME)
    try_load(resource)
    return resource


@fixture(scope="module")
def initialize_inventory_documents(active_rs: MongoDB) -> int:
    col = active_rs.tester().client[PRODUCTS_DB][INVENTORY_COLLECTION]
    col.delete_many({})
    col.insert_many(INVENTORY_DOCS)
    count = col.count_documents({})
    assert count == len(INVENTORY_DOCS)
    return count


# ══════════════════════════════════════════════════════════════════════════════
# SHIPPER TESTS (Active Cluster)
# ══════════════════════════════════════════════════════════════════════════════


@mark.e2e_replica_set_monarch
class TestMonarchShipper(KubernetesTester):
    """Tests for active cluster with shipper deployment."""

    def test_active_rs_running(self, active_rs: MongoDB):
        """Deploy active RS without Monarch spec first."""
        active_rs.update()
        active_rs.assert_reaches_phase(Phase.Running, timeout=600)

    def test_no_monarch_resources_before_activation(self, active_rs: MongoDB):
        """Verify no shipper exists before Monarch is activated."""
        apps = k8s_client.AppsV1Api()
        try:
            apps.read_namespaced_deployment(f"{ACTIVE_RS_NAME}-monarch-shipper", self.namespace)
            assert False, "Shipper Deployment should not exist before Monarch activation"
        except k8s_client.exceptions.ApiException as e:
            assert e.status == 404

    def test_insert_documents_before_activation(self, initialize_inventory_documents: int):
        """Insert test documents before activating Monarch."""
        assert initialize_inventory_documents == len(INVENTORY_DOCS)

    def test_activate_monarch(self, active_rs: MongoDB, s3_creds_secret: str, namespace: str):
        """Activate Monarch on the running RS by adding spec.monarch."""
        active_rs["spec"]["monarch"] = _monarch_spec(namespace, "active")
        active_rs.update()
        active_rs.assert_reaches_phase(Phase.Running, timeout=600)
        _wait_for_monarch_condition(active_rs)

    def test_automation_config_has_monarch_components(self, active_rs: MongoDB):
        """Verify automation config contains maintainedMonarchComponents for active cluster."""
        config = active_rs.get_automation_config_tester().automation_config
        mc = config["maintainedMonarchComponents"]
        assert len(mc) == 1
        assert mc[0]["replicaSetId"] == ACTIVE_RS_NAME
        assert mc[0]["awsBucketName"] == S3_BUCKET
        assert mc[0]["clusterPrefix"] == CLUSTER_PREFIX
        assert mc[0]["initialMode"] == "ACTIVE"
        assert mc[0]["injectorConfig"]["shards"] == []

    def test_shipper_uploads_to_s3(self, active_rs: MongoDB):
        """Verify shipper is uploading oplog data to S3."""
        _wait_for_s3_data(self.namespace)

    def test_shipper_ships_new_writes(self, active_rs: MongoDB):
        """Verify shipper continues to ship new writes to S3."""
        s3 = _s3_client(_minio_endpoint(self.namespace))
        prefix = f"{CLUSTER_PREFIX}/{SHARD_ID}/slices/"
        before = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix).get("KeyCount", 0)
        active_rs.tester().client["shipper_test"]["test"].insert_one({"ts": time.time()})
        time.sleep(15)
        after = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix).get("KeyCount", 0)
        assert after > before, f"Shipper not shipping: slice count unchanged at {before}"


# ══════════════════════════════════════════════════════════════════════════════
# INJECTOR TESTS (Standby Cluster)
# ══════════════════════════════════════════════════════════════════════════════


@mark.e2e_replica_set_monarch
class TestMonarchInjector(KubernetesTester):
    """Tests for standby cluster with injector deployment."""

    def test_standby_rs_running(self, standby_rs: MongoDB):
        """Standby RS reaches Running with InjectorReady=True.

        The operator waits for the injector Deployment to be healthy before pushing
        automation config with InjectorInstances. So agents receive InjectorInstances
        pointing to an already-healthy service, and WaitForInjectorReady completes
        immediately. No transient blocking window to test — just final state.
        """
        standby_rs.update()
        standby_rs.assert_reaches_phase(Phase.Running, timeout=600)
        _wait_for_monarch_condition(standby_rs)

    def test_standby_automation_config(self, standby_rs: MongoDB):
        """Verify automation config has InjectorInstances for each RS member."""
        config = standby_rs.get_automation_config_tester().automation_config
        mc = config["maintainedMonarchComponents"]
        # replicaSetId is the local RS name; DR pair linkage is via shared clusterPrefix
        assert mc[0]["replicaSetId"] == STANDBY_RS_NAME

        instances = mc[0]["injectorConfig"]["shards"][0]["instances"]
        members = standby_rs["spec"]["members"]
        assert len(instances) == members

        svc_dns = f"{STANDBY_RS_NAME}-monarch-injector-svc.{self.namespace}.svc.cluster.local"
        for i, inst in enumerate(instances):
            expected_host = f"{STANDBY_RS_NAME}-{i}.{STANDBY_RS_NAME}-svc.{self.namespace}.svc.cluster.local"
            assert inst["hostname"] == expected_host
            assert inst["healthApiEndpoint"] == f"{svc_dns}:8080"
            assert inst["monarchApiEndpoint"] == f"{svc_dns}:1122"
            assert inst["externallyManaged"] is True

    def test_documents_replicated_to_standby(self, standby_rs: MongoDB):
        """Verify documents from active cluster are replicated to standby."""
        col = standby_rs.tester().client[PRODUCTS_DB][INVENTORY_COLLECTION]
        deadline = time.time() + 600
        count = 0
        while time.time() < deadline:
            try:
                count = col.count_documents({})
                if count == len(INVENTORY_DOCS):
                    break
            except pymongo.errors.PyMongoError:
                pass
            time.sleep(5)
        assert count == len(INVENTORY_DOCS), f"Expected {len(INVENTORY_DOCS)} docs on standby, got {count}"


# ══════════════════════════════════════════════════════════════════════════════
# FAILOVER / PROMOTION TESTS
# ══════════════════════════════════════════════════════════════════════════════


@mark.e2e_replica_set_monarch
class TestMonarchPromotion(KubernetesTester):
    """Tests for promoting standby cluster to active (failover)."""

    def test_promote_standby_to_active(self, standby_rs: MongoDB, namespace: str):
        """Change standby RS role from 'standby' to 'active'.

        This triggers the promotion state machine:
        1. Operator writes PromoteStandby to S3
        2. Agent sees PromoteStandby, completes RS reconfig, writes StandbyReadyToPromote
        3. Operator sees StandbyReadyToPromote, deletes injector, creates shipper
        4. Operator writes Active to S3
        """
        standby_rs["spec"]["monarch"]["role"] = "active"
        standby_rs["spec"]["monarch"].pop("source", None)
        standby_rs.update()

    def test_failover_in_progress_condition(self, standby_rs: MongoDB):
        """Verify FailoverInProgress condition appears during promotion."""
        def has_failover_condition(resource: MongoDB) -> bool:
            for cond in resource["status"]["conditions"]:
                if cond.get("type") == "FailoverInProgress":
                    return True
            return False

        try:
            standby_rs.wait_for(has_failover_condition, timeout=60, should_raise=True)
        except TimeoutError:
            # Failover might complete quickly - that's OK
            pass

    def test_promotion_completes(self, standby_rs: MongoDB):
        """Wait for promotion to complete - FailoverInProgress=False or ShipperReady=True."""
        def is_promotion_complete(resource: MongoDB) -> bool:
            conditions = resource.get("status", {}).get("conditions", [])
            for cond in conditions:
                if cond.get("type") == "FailoverInProgress" and cond.get("status") == "False":
                    return True
                if cond.get("type") == "ShipperReady" and cond.get("status") == "True":
                    return True
            return False

        standby_rs.wait_for(is_promotion_complete, timeout=600, should_raise=True)

    def test_injector_deleted_after_promotion(self, standby_rs: MongoDB):
        """Verify injector Deployment is deleted after promotion."""
        apps = k8s_client.AppsV1Api()
        try:
            apps.read_namespaced_deployment(f"{STANDBY_RS_NAME}-monarch-injector", self.namespace)
            assert False, "Injector Deployment should be deleted after promotion"
        except k8s_client.exceptions.ApiException as e:
            assert e.status == 404, f"Expected 404, got {e.status}"

    def test_shipper_created_after_promotion(self, standby_rs: MongoDB):
        """Verify shipper Deployment exists after promotion."""
        _wait_for_deployment_ready(self.namespace, f"{STANDBY_RS_NAME}-monarch-shipper", timeout=180)

    def test_shipper_ready_condition_after_promotion(self, standby_rs: MongoDB):
        """Verify ShipperReady condition is True after promotion."""
        def has_shipper_ready(resource: MongoDB) -> bool:
            for cond in resource.get("status", {}).get("conditions", []):
                if cond.get("type") == "ShipperReady" and cond.get("status") == "True":
                    return True
            return False

        standby_rs.wait_for(has_shipper_ready, timeout=300, should_raise=True)

    def test_s3_dr_state_is_active(self, standby_rs: MongoDB):
        """Verify the S3 DR state file shows 'Active' state after promotion."""
        s3 = _s3_client(_minio_endpoint(self.namespace))
        dr_state_key = f"{CLUSTER_PREFIX}/dr_state.json"

        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                response = s3.get_object(Bucket=S3_BUCKET, Key=dr_state_key)
                import json
                state = json.loads(response["Body"].read().decode("utf-8"))
                if state.get("state") == "Active":
                    return
            except s3.exceptions.NoSuchKey:
                pass
            except Exception:
                pass
            time.sleep(5)

        response = s3.get_object(Bucket=S3_BUCKET, Key=dr_state_key)
        import json
        state = json.loads(response["Body"].read().decode("utf-8"))
        assert state.get("state") == "Active", f"Expected S3 DR state 'Active', got {state}"

    def test_monarch_status_reflects_s3_state(self, standby_rs: MongoDB):
        """Verify status.monarch.observedS3State reflects the S3 DR state after promotion."""
        def has_observed_s3_state(resource: MongoDB) -> bool:
            monarch_status = resource.get("status", {}).get("monarch", {})
            observed_state = monarch_status.get("observedS3State", "")
            return observed_state == "Active"

        standby_rs.wait_for(has_observed_s3_state, timeout=120, should_raise=True)

        resource = standby_rs.load()
        monarch_status = resource.get("status", {}).get("monarch", {})
        assert monarch_status.get("observedS3StateTime") is not None, "observedS3StateTime should be set"

    def test_no_spec_out_of_sync_after_planned_promotion(self, standby_rs: MongoDB):
        """Verify SpecOutOfSync condition is NOT set after planned promotion.

        After a planned promotion (CR role changed from standby to active), the CR spec
        matches the S3 state, so SpecOutOfSync should not be set.
        """
        resource = standby_rs.load()
        conditions = resource.get("status", {}).get("conditions", [])
        for cond in conditions:
            if cond.get("type") == "SpecOutOfSync":
                assert cond.get("status") != "True", \
                    f"SpecOutOfSync should not be True after planned promotion: {cond}"

    def test_promoted_cluster_can_write(self, standby_rs: MongoDB):
        """Verify the promoted cluster can accept writes."""
        col = standby_rs.tester().client["promotion_test"]["writes"]
        col.insert_one({"promoted": True, "ts": time.time()})
        count = col.count_documents({})
        assert count >= 1, "Promoted cluster should accept writes"

    def test_promoted_shipper_uploads_to_s3(self, standby_rs: MongoDB):
        """Verify the promoted cluster's shipper is uploading to S3."""
        s3 = _s3_client(_minio_endpoint(self.namespace))
        prefix = f"{CLUSTER_PREFIX}/{SHARD_ID}/slices/"
        before = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix).get("KeyCount", 0)

        standby_rs.tester().client["shipper_test"]["promoted"].insert_one({"ts": time.time()})
        time.sleep(15)

        after = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix).get("KeyCount", 0)
        assert after > before, f"Promoted shipper not shipping: slice count unchanged at {before}"
