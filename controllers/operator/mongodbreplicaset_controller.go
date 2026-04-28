package operator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mdbv1 "github.com/mongodb/mongodb-kubernetes/api/v1/mdb"
	rolev1 "github.com/mongodb/mongodb-kubernetes/api/v1/role"
	searchv1 "github.com/mongodb/mongodb-kubernetes/api/v1/search"
	mdbstatus "github.com/mongodb/mongodb-kubernetes/api/v1/status"
	"github.com/mongodb/mongodb-kubernetes/controllers/om"
	"github.com/mongodb/mongodb-kubernetes/controllers/om/backup"
	"github.com/mongodb/mongodb-kubernetes/controllers/om/deployment"
	"github.com/mongodb/mongodb-kubernetes/controllers/om/host"
	"github.com/mongodb/mongodb-kubernetes/controllers/om/replicaset"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/agents"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/certs"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/connection"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/construct"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/controlledfeature"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/create"
	enterprisepem "github.com/mongodb/mongodb-kubernetes/controllers/operator/pem"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/project"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/recovery"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/watch"
	"github.com/mongodb/mongodb-kubernetes/controllers/operator/workflow"
	"github.com/mongodb/mongodb-kubernetes/controllers/searchcontroller"
	mcoConstruct "github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/controllers/construct"
	"github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/pkg/kube/annotations"
	kubernetesClient "github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/pkg/kube/client"
	"github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/pkg/kube/configmap"
	"github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/pkg/kube/container"
	"github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/pkg/util/merge"
	"github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/pkg/util/scale"
	"github.com/mongodb/mongodb-kubernetes/pkg/dns"
	"github.com/mongodb/mongodb-kubernetes/pkg/images"
	"github.com/mongodb/mongodb-kubernetes/pkg/kube"
	"github.com/mongodb/mongodb-kubernetes/pkg/monarch/drstate"
	"github.com/mongodb/mongodb-kubernetes/pkg/statefulset"
	"github.com/mongodb/mongodb-kubernetes/pkg/util"
	"github.com/mongodb/mongodb-kubernetes/pkg/util/architectures"
	"github.com/mongodb/mongodb-kubernetes/pkg/util/env"
	util_int "github.com/mongodb/mongodb-kubernetes/pkg/util/int"
	"github.com/mongodb/mongodb-kubernetes/pkg/util/maputil"
	"github.com/mongodb/mongodb-kubernetes/pkg/vault"
	"github.com/mongodb/mongodb-kubernetes/pkg/vault/vaultwatcher"
	"k8s.io/utils/ptr"
)

// ReconcileMongoDbReplicaSet reconciles a MongoDB with a type of ReplicaSet.
// WARNING: do not put any mutable state into this struct.
// Controller runtime uses and shares a single instance of it.
type ReconcileMongoDbReplicaSet struct {
	*ReconcileCommonController
	omConnectionFactory       om.ConnectionFactory
	imageUrls                 images.ImageUrls
	forceEnterprise           bool
	enableClusterMongoDBRoles bool

	initDatabaseNonStaticImageVersion string
	databaseNonStaticImageVersion     string

	agentDebug      bool
	agentDebugImage string
}

type replicaSetDeploymentState struct {
	LastAchievedSpec         *mdbv1.MongoDbSpec
	LastReconcileMemberCount int
	LastConfiguredRoles      []string
}

var _ reconcile.Reconciler = &ReconcileMongoDbReplicaSet{}

// ReplicaSetReconcilerHelper contains state and logic for a SINGLE reconcile execution.
// This object is NOT shared between reconcile invocations.
type ReplicaSetReconcilerHelper struct {
	resource        *mdbv1.MongoDB
	deploymentState *replicaSetDeploymentState
	reconciler      *ReconcileMongoDbReplicaSet
	log             *zap.SugaredLogger
}

func (r *ReconcileMongoDbReplicaSet) newReconcilerHelper(
	ctx context.Context,
	rs *mdbv1.MongoDB,
	log *zap.SugaredLogger,
) (*ReplicaSetReconcilerHelper, error) {
	helper := &ReplicaSetReconcilerHelper{
		resource:   rs,
		reconciler: r,
		log:        log,
	}

	if err := helper.initialize(ctx); err != nil {
		return nil, err
	}

	return helper, nil
}

// readState abstract reading the state of the resource that we store on the cluster between reconciliations.
func (r *ReplicaSetReconcilerHelper) readState() (*replicaSetDeploymentState, error) {
	// Try to get the last achieved spec from annotations and store it in state
	lastAchievedSpec, err := r.resource.GetLastSpec()
	if err != nil {
		return nil, err
	}

	// Read current member count from Status once at initialization. This provides a stable view throughout
	// reconciliation and prepares for eventually storing this in ConfigMap state instead of ephemeral status.
	lastReconcileMemberCount := r.resource.Status.Members

	lastConfiguredRoles, err := r.resource.GetLastConfiguredRoles()
	if err != nil {
		return nil, err
	}

	return &replicaSetDeploymentState{
		LastAchievedSpec:         lastAchievedSpec,
		LastReconcileMemberCount: lastReconcileMemberCount,
		LastConfiguredRoles:      lastConfiguredRoles,
	}, nil
}

// getVaultAnnotations gets vault secret version annotations to write to the CR.
func (r *ReplicaSetReconcilerHelper) getVaultAnnotations() map[string]string {
	if !vault.IsVaultSecretBackend() {
		return nil
	}

	vaultMap := make(map[string]string)
	secrets := r.resource.GetSecretsMountedIntoDBPod()

	for _, s := range secrets {
		path := fmt.Sprintf("%s/%s/%s", r.reconciler.VaultClient.DatabaseSecretMetadataPath(),
			r.resource.Namespace, s)
		vaultMap = merge.StringToStringMap(vaultMap, r.reconciler.VaultClient.GetSecretAnnotation(path))
	}

	path := fmt.Sprintf("%s/%s/%s", r.reconciler.VaultClient.OperatorScretMetadataPath(),
		r.resource.Namespace, r.resource.Spec.Credentials)
	vaultMap = merge.StringToStringMap(vaultMap, r.reconciler.VaultClient.GetSecretAnnotation(path))

	return vaultMap
}

func (r *ReplicaSetReconcilerHelper) initialize(ctx context.Context) error {
	state, err := r.readState()
	if err != nil {
		return xerrors.Errorf("failed to initialize replica set state: %w", err)
	}
	r.deploymentState = state
	return nil
}

// updateStatus is a pass-through method that calls the reconciler updateStatus.
// In the future (multi-cluster epic), this will be enhanced to write deployment state to ConfigMap after every status
// update (similar to sharded cluster pattern), but for now it just delegates to maintain the same architecture.
func (r *ReplicaSetReconcilerHelper) updateStatus(ctx context.Context, status workflow.Status, statusOptions ...mdbstatus.Option) (reconcile.Result, error) {
	return r.reconciler.updateStatus(ctx, r.resource, status, r.log, statusOptions...)
}

// Reconcile performs the full reconciliation logic for a replica set.
// This is the main entry point for all reconciliation work and contains all
// state and logic specific to a single reconcile execution.
func (r *ReplicaSetReconcilerHelper) Reconcile(ctx context.Context) (reconcile.Result, error) {
	rs := r.resource
	log := r.log
	reconciler := r.reconciler

	// === 1. Initial Checks and setup
	if !architectures.IsRunningStaticArchitecture(rs.Annotations) {
		agents.UpgradeAllIfNeeded(ctx, agents.ClientSecret{Client: reconciler.client, SecretClient: reconciler.SecretClient}, reconciler.omConnectionFactory, GetWatchedNamespace(), false)
	}

	log.Info("-> ReplicaSet.Reconcile")
	log.Infow("ReplicaSet.Spec", "spec", rs.Spec, "desiredReplicas", scale.ReplicasThisReconciliation(rs), "isScaling", scale.IsStillScaling(rs))
	log.Infow("ReplicaSet.Status", "status", rs.Status)

	if err := rs.ProcessValidationsOnReconcile(nil); err != nil {
		return r.updateStatus(ctx, workflow.Invalid("%s", err.Error()))
	}

	projectConfig, credsConfig, err := project.ReadConfigAndCredentials(ctx, reconciler.client, reconciler.SecretClient, rs, log)
	if err != nil {
		return r.updateStatus(ctx, workflow.Failed(err))
	}

	conn, _, err := connection.PrepareOpsManagerConnection(ctx, reconciler.SecretClient, projectConfig, credsConfig, reconciler.omConnectionFactory, rs.Namespace, log)
	if err != nil {
		return r.updateStatus(ctx, workflow.Failed(xerrors.Errorf("failed to prepare Ops Manager connection: %w", err)))
	}

	reconciler.SetupCommonWatchers(rs, nil, nil, rs.Name)

	reconcileResult := checkIfHasExcessProcesses(conn, rs.Name, log)
	if !reconcileResult.IsOK() {
		return r.updateStatus(ctx, reconcileResult)
	}

	if status := validateMongoDBResource(rs, conn); !status.IsOK() {
		return r.updateStatus(ctx, status)
	}

	if status := controlledfeature.EnsureFeatureControls(*rs, conn, conn.OpsManagerVersion(), log); !status.IsOK() {
		return r.updateStatus(ctx, status)
	}

	// === 2. Auth and Certificates
	// Get certificate paths for later use
	rsCertsConfig := certs.ReplicaSetConfig(*rs)
	var databaseSecretPath string
	if reconciler.VaultClient != nil {
		databaseSecretPath = reconciler.VaultClient.DatabaseSecretPath()
	}
	tlsCertHash := enterprisepem.ReadHashFromSecret(ctx, reconciler.SecretClient, rs.Namespace, rsCertsConfig.CertSecretName, databaseSecretPath, log)
	internalClusterCertHash := enterprisepem.ReadHashFromSecret(ctx, reconciler.SecretClient, rs.Namespace, rsCertsConfig.InternalClusterSecretName, databaseSecretPath, log)

	tlsCertPath := ""
	internalClusterCertPath := ""
	if internalClusterCertHash != "" {
		internalClusterCertPath = fmt.Sprintf("%s%s", util.InternalClusterAuthMountPath, internalClusterCertHash)
	}
	if tlsCertHash != "" {
		tlsCertPath = fmt.Sprintf("%s/%s", util.TLSCertMountPath, tlsCertHash)
	}

	agentCertSecretName := rs.GetSecurity().AgentClientCertificateSecretName(rs.Name)
	agentCertHash, agentCertPath := reconciler.agentCertHashAndPath(ctx, log, rs.Namespace, agentCertSecretName, databaseSecretPath)

	prometheusCertHash, err := certs.EnsureTLSCertsForPrometheus(ctx, reconciler.SecretClient, rs.GetNamespace(), rs.GetPrometheus(), certs.Database, log)
	if err != nil {
		return r.updateStatus(ctx, workflow.Failed(xerrors.Errorf("could not generate certificates for Prometheus: %w", err)))
	}

	currentAgentAuthMode, err := conn.GetAgentAuthMode()
	if err != nil {
		return r.updateStatus(ctx, workflow.Failed(xerrors.Errorf("failed to get agent auth mode: %w", err)))
	}

	// Check if we need to prepare for scale-down
	if scale.ReplicasThisReconciliation(rs) < r.deploymentState.LastReconcileMemberCount {
		if err := replicaset.PrepareScaleDownFromMongoDB(conn, rs, log); err != nil {
			return r.updateStatus(ctx, workflow.Failed(xerrors.Errorf("failed to prepare Replica Set for scaling down using Ops Manager: %w", err)))
		}
	}
	deploymentOpts := deploymentOptionsRS{
		prometheusCertHash:   prometheusCertHash,
		agentCertPath:        agentCertPath,
		agentCertHash:        agentCertHash,
		currentAgentAuthMode: currentAgentAuthMode,
	}

	// 3. Search Overrides
	// Apply search overrides early so searchCoordinator role is present before ensureRoles runs
	// This must happen before the ordering logic to ensure roles are synced regardless of order
	shouldMirrorKeyfileForMongot, err := r.applySearchOverrides(ctx)
	if err != nil {
		return r.updateStatus(ctx, workflow.Failed(err))
	}

	// 4. Recovery
	// Recovery prevents some deadlocks that can occur during reconciliation, e.g. the setting of an incorrect automation
	// configuration and a subsequent attempt to overwrite it later, the operator would be stuck in Pending phase.
	// See CLOUDP-189433 and CLOUDP-229222 for more details.
	if recovery.ShouldTriggerRecovery(rs.Status.Phase != mdbstatus.PhaseRunning, rs.Status.LastTransition) {
		log.Warnf("Triggering Automatic Recovery. The MongoDB resource %s/%s is in %s state since %s", rs.Namespace, rs.Name, rs.Status.Phase, rs.Status.LastTransition)
		automationConfigStatus := r.updateOmDeploymentRs(ctx, conn, r.deploymentState.LastReconcileMemberCount, tlsCertPath, internalClusterCertPath, deploymentOpts, shouldMirrorKeyfileForMongot, true).OnErrorPrepend("failed to create/update (Ops Manager reconciliation phase):")
		reconcileStatus := r.reconcileMemberResources(ctx, conn, projectConfig, deploymentOpts, r.deploymentState.LastConfiguredRoles)
		if !reconcileStatus.IsOK() {
			log.Errorf("Recovery failed because of reconcile errors, %v", reconcileStatus)
		}
		if !automationConfigStatus.IsOK() {
			log.Errorf("Recovery failed because of Automation Config update errors, %v", automationConfigStatus)
		}
	}

	// 5. Actual reconciliation execution, Ops Manager and kubernetes resources update
	publishAutomationConfigFirst := publishAutomationConfigFirstRS(ctx, reconciler.client, *rs, r.deploymentState.LastAchievedSpec, deploymentOpts.currentAgentAuthMode, projectConfig.SSLMMSCAConfigMap, log)
	status := workflow.RunInGivenOrder(publishAutomationConfigFirst,
		func() workflow.Status {
			return r.updateOmDeploymentRs(ctx, conn, r.deploymentState.LastReconcileMemberCount, tlsCertPath, internalClusterCertPath, deploymentOpts, shouldMirrorKeyfileForMongot, false).OnErrorPrepend("failed to create/update (Ops Manager reconciliation phase):")
		},
		func() workflow.Status {
			return r.reconcileMemberResources(ctx, conn, projectConfig, deploymentOpts, r.deploymentState.LastConfiguredRoles)
		})

	if !status.IsOK() {
		return r.updateStatus(ctx, status)
	}

	// === 5b. Monarch reconciliation (Deployments, Services, and automation config)
	if rs.Spec.Monarch != nil {
		if monarchStatus := r.reconcileMonarch(ctx, conn); !monarchStatus.IsOK() {
			return r.updateStatus(ctx, monarchStatus)
		}
	} else {
		// Monarch was removed — clean up K8s resources.
		if cleanupStatus := r.cleanupMonarchResources(ctx); !cleanupStatus.IsOK() {
			return r.updateStatus(ctx, cleanupStatus)
		}
		apimeta.RemoveStatusCondition(&rs.Status.Conditions, mdbv1.ConditionShipperReady)
		apimeta.RemoveStatusCondition(&rs.Status.Conditions, mdbv1.ConditionInjectorReady)
	}

	// === 6. Final steps
	if scale.IsStillScaling(rs) {
		return r.updateStatus(ctx, workflow.Pending("Continuing scaling operation for ReplicaSet %s, desiredMembers=%d, currentMembers=%d", rs.ObjectKey(), rs.DesiredReplicas(), scale.ReplicasThisReconciliation(rs)), mdbstatus.MembersOption(rs))
	}

	// Get lastspec, vault annotations when needed and write them to the resource.
	// These operations should only be performed on successful reconciliations.
	// The state of replica sets is currently split between the annotations and the member count in status. Both should
	// be migrated to config maps
	annotationsToAdd, err := getAnnotationsForResource(r.resource)
	if err != nil {
		return r.updateStatus(ctx, workflow.Failed(xerrors.Errorf("could not get resource annotations: %w", err)))
	}

	for k, val := range r.getVaultAnnotations() {
		annotationsToAdd[k] = val
	}

	roleAnnotation, _, err := r.reconciler.getRoleAnnotation(ctx, r.resource.Spec.DbCommonSpec, r.reconciler.enableClusterMongoDBRoles, kube.ObjectKeyFromApiObject(r.resource))
	if err != nil {
		return r.updateStatus(ctx, workflow.Failed(err))
	}
	for k, val := range roleAnnotation {
		annotationsToAdd[k] = val
	}

	if err := annotations.SetAnnotations(ctx, r.resource, annotationsToAdd, r.reconciler.client); err != nil {
		return r.updateStatus(ctx, workflow.Failed(xerrors.Errorf("could not update resource annotations: %w", err)))
	}

	log.Infof("Finished reconciliation for MongoDbReplicaSet! %s", completionMessage(conn.BaseURL(), conn.GroupID()))
	return r.updateStatus(ctx, workflow.OK(), mdbstatus.NewBaseUrlOption(deployment.Link(conn.BaseURL(), conn.GroupID())), mdbstatus.MembersOption(rs), mdbstatus.NewPVCsStatusOptionEmptyStatus())
}

func newReplicaSetReconciler(ctx context.Context, kubeClient client.Client, imageUrls images.ImageUrls, initDatabaseNonStaticImageVersion, databaseNonStaticImageVersion string, forceEnterprise, enableClusterMongoDBRoles, agentDebug bool, agentDebugImage string, omFunc om.ConnectionFactory) *ReconcileMongoDbReplicaSet {
	return &ReconcileMongoDbReplicaSet{
		ReconcileCommonController: NewReconcileCommonController(ctx, kubeClient),
		omConnectionFactory:       omFunc,
		imageUrls:                 imageUrls,
		forceEnterprise:           forceEnterprise,
		enableClusterMongoDBRoles: enableClusterMongoDBRoles,

		initDatabaseNonStaticImageVersion: initDatabaseNonStaticImageVersion,
		databaseNonStaticImageVersion:     databaseNonStaticImageVersion,

		agentDebug:      agentDebug,
		agentDebugImage: agentDebugImage,
	}
}

type deploymentOptionsRS struct {
	agentCertPath        string
	agentCertHash        string
	prometheusCertHash   string
	currentAgentAuthMode string
}

// Generic Kubernetes Resources
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=list;watch,namespace=placeholder
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch,namespace=placeholder
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update,namespace=placeholder
// +kubebuilder:rbac:groups=core,resources={secrets,configmaps},verbs=get;list;watch;create;delete;update,namespace=placeholder
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=create;get;list;watch;delete;update,namespace=placeholder

// MongoDB Resource
// +kubebuilder:rbac:groups=mongodb.com,resources={mongodb,mongodb/status,mongodb/finalizers},verbs=*,namespace=placeholder

// Setting up a webhook
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingwebhookconfigurations,verbs=get;create;update;delete

// Certificate generation
// +kubebuilder:rbac:groups=certificates.k8s.io,resources=certificatesigningrequests,verbs=get;create;list;watch

// Reconcile reads that state of the cluster for a MongoDbReplicaSet object and makes changes based on the state read
// and what is in the MongoDbReplicaSet.Spec
func (r *ReconcileMongoDbReplicaSet) Reconcile(ctx context.Context, request reconcile.Request) (res reconcile.Result, e error) {
	log := zap.S().With("ReplicaSet", request.NamespacedName)
	rs := &mdbv1.MongoDB{}

	if reconcileResult, err := r.prepareResourceForReconciliation(ctx, request, rs, log); err != nil {
		if errors.IsNotFound(err) {
			return workflow.Invalid("Object for reconciliation not found").ReconcileResult()
		}
		return reconcileResult, err
	}

	// Create helper for THIS reconciliation
	helper, err := r.newReconcilerHelper(ctx, rs, log)
	if err != nil {
		return r.updateStatus(ctx, rs, workflow.Failed(err), log)
	}

	// Delegate all reconciliation logic to helper
	return helper.Reconcile(ctx)
}

func publishAutomationConfigFirstRS(ctx context.Context, getter kubernetesClient.Client, mdb mdbv1.MongoDB, lastSpec *mdbv1.MongoDbSpec, currentAgentAuthMode string, sslMMSCAConfigMap string, log *zap.SugaredLogger) bool {
	namespacedName := kube.ObjectKey(mdb.Namespace, mdb.Name)
	currentSts, err := getter.GetStatefulSet(ctx, namespacedName)
	if err != nil {
		if errors.IsNotFound(err) {
			// No need to publish state as this is a new StatefulSet
			log.Debugf("New StatefulSet %s", namespacedName)
			return false
		}

		log.Debugw(fmt.Sprintf("Error getting StatefulSet %s", namespacedName), "error", err)
		return false
	}

	databaseContainer := container.GetByName(util.DatabaseContainerName, currentSts.Spec.Template.Spec.Containers)
	volumeMounts := databaseContainer.VolumeMounts

	if !mdb.Spec.Security.IsTLSEnabled() && wasTLSSecretMounted(ctx, getter, currentSts, mdb, log) {
		log.Debug(automationConfigFirstMsg("security.tls.enabled", "false"))
		return true
	}

	if mdb.Spec.Security.TLSConfig.CA == "" && wasCAConfigMapMounted(ctx, getter, currentSts, mdb, log) {
		log.Debug(automationConfigFirstMsg("security.tls.CA", "empty"))
		return true
	}

	if sslMMSCAConfigMap == "" && statefulset.VolumeMountWithNameExists(volumeMounts, construct.CaCertName) {
		log.Debug(automationConfigFirstMsg("SSLMMSCAConfigMap", "empty"))
		return true
	}

	if mdb.Spec.Security.GetAgentMechanism(currentAgentAuthMode) != util.X509 && statefulset.VolumeMountWithNameExists(volumeMounts, util.AgentSecretName) {
		log.Debug(automationConfigFirstMsg("project.AuthMode", "empty"))
		return true
	}

	if mdb.Spec.Members < int(*currentSts.Spec.Replicas) {
		log.Debug("Scaling down operation. automationConfig needs to be updated first")
		return true
	}

	if architectures.IsRunningStaticArchitecture(mdb.GetAnnotations()) {
		if mdb.Spec.IsInChangeVersion(lastSpec) {
			return true
		}
	}

	return false
}

func getHostnameOverrideConfigMapForReplicaset(mdb *mdbv1.MongoDB) corev1.ConfigMap {
	data := make(map[string]string)

	if mdb.Spec.DbCommonSpec.GetExternalDomain() != nil {
		hostnames, names := dns.GetDNSNames(mdb.Name, "", mdb.GetObjectMeta().GetNamespace(), mdb.Spec.GetClusterDomain(), mdb.Spec.Members, mdb.Spec.DbCommonSpec.GetExternalDomain())
		for i := range hostnames {
			data[names[i]] = hostnames[i]
		}
	}

	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-hostname-override", mdb.Name),
			Namespace: mdb.Namespace,
		},
		Data: data,
	}
	return cm
}

func (r *ReplicaSetReconcilerHelper) reconcileHostnameOverrideConfigMap(ctx context.Context, log *zap.SugaredLogger, getUpdateCreator configmap.GetUpdateCreator) error {
	if r.resource.Spec.DbCommonSpec.GetExternalDomain() == nil {
		return nil
	}

	cm := getHostnameOverrideConfigMapForReplicaset(r.resource)
	err := configmap.CreateOrUpdate(ctx, getUpdateCreator, cm)
	if err != nil && !errors.IsAlreadyExists(err) {
		return xerrors.Errorf("failed to create configmap: %s, err: %w", cm.Name, err)
	}
	log.Infof("Successfully ensured configmap: %s", cm.Name)

	return nil
}

// reconcileMemberResources handles the synchronization of kubernetes resources, which can be statefulsets, services etc.
// All the resources required in the k8s cluster (as opposed to the automation config) for creating the replicaset
// should be reconciled in this method.
func (r *ReplicaSetReconcilerHelper) reconcileMemberResources(ctx context.Context, conn om.Connection, projectConfig mdbv1.ProjectConfig, deploymentOptions deploymentOptionsRS, lastConfiguredRoles []string) workflow.Status {
	rs := r.resource
	reconciler := r.reconciler
	log := r.log

	// Reconcile hostname override ConfigMap
	if err := r.reconcileHostnameOverrideConfigMap(ctx, log, r.reconciler.client); err != nil {
		return workflow.Failed(xerrors.Errorf("failed to reconcile hostname override ConfigMap: %w", err))
	}

	// Ensure roles are properly configured
	if status := reconciler.ensureRoles(ctx, rs.Spec.DbCommonSpec, reconciler.enableClusterMongoDBRoles, conn, kube.ObjectKeyFromApiObject(rs), lastConfiguredRoles, log); !status.IsOK() {
		return status
	}

	return r.reconcileStatefulSet(ctx, conn, projectConfig, deploymentOptions)
}

// reconcileMonarch ensures Monarch ConfigMap, Deployment, Service, and automation config are up to date.
// Order: ConfigMap → Service → Deployment → wait for ready → push automation config.
// This ordering ensures agents only see InjectorInstances when the injector is already healthy,
// eliminating the race where agents block in WaitForInjectorReady on an unhealthy service.
//
// If a role change is detected (active ↔ standby), this function delegates to reconcileMonarchFailover
// to handle the failover state machine before proceeding with normal reconciliation.
func (r *ReplicaSetReconcilerHelper) reconcileMonarch(ctx context.Context, conn om.Connection) workflow.Status {
	rs := r.resource
	reconciler := r.reconciler
	log := r.log

	// Check for role change (failover trigger)
	if previousRole := r.detectMonarchRoleChange(); previousRole != nil {
		log.Infow("Monarch role change detected", "from", *previousRole, "to", rs.Spec.Monarch.Role)
		if failoverStatus := r.reconcileMonarchFailover(ctx, *previousRole); !failoverStatus.IsOK() {
			return failoverStatus
		}
		// Failover completed - continue with normal reconciliation to create new resources
	}

	// Read S3 DR state on every reconcile and check for unplanned failover.
	// This handles the case where an external tool (CLI) writes to S3 directly without updating the CR.
	if status := r.reconcileMonarchS3State(ctx); !status.IsOK() {
		return status
	}

	// Determine role early for condition type and resource naming
	conditionType := mdbv1.ConditionInjectorReady
	monarchRole := "injector"
	if rs.Spec.Monarch.Role == mdbv1.MonarchRoleActive {
		conditionType = mdbv1.ConditionShipperReady
		monarchRole = "shipper"
	}
	role := string(rs.Spec.Monarch.Role)

	// 0. Read AWS credentials early (fail fast if secret doesn't exist)
	credSecret, err := reconciler.client.GetSecret(ctx, kube.ObjectKey(rs.Namespace, rs.Spec.Monarch.S3.CredentialsSecretRef.Name))
	if err != nil {
		return workflow.Failed(xerrors.Errorf("failed to read Monarch credentials secret %s: %w", rs.Spec.Monarch.S3.CredentialsSecretRef.Name, err))
	}
	awsKeyId := string(credSecret.Data["awsAccessKeyId"])
	awsSecret := string(credSecret.Data["awsSecretAccessKey"])

	// Build MongoDB connection string for the replica set
	srcURI := buildMongoDBConnectionString(rs)

	// 1. Create or update Monarch ConfigMap (contains YAML config)
	cm := construct.BuildMonarchConfigMap(rs, rs.Namespace, srcURI)
	if _, err := controllerutil.CreateOrUpdate(ctx, reconciler.client, cm, func() error {
		fresh := construct.BuildMonarchConfigMap(rs, rs.Namespace, srcURI)
		cm.Data = fresh.Data
		cm.Labels = fresh.Labels
		return nil
	}); err != nil {
		return workflow.Failed(xerrors.Errorf("failed to create/update Monarch ConfigMap %s: %w", cm.Name, err))
	}

	// 2. Create or update Monarch Service (before Deployment so DNS is resolvable)
	svc := construct.BuildMonarchService(rs, rs.Namespace)
	if _, err := controllerutil.CreateOrUpdate(ctx, reconciler.client, svc, func() error {
		fresh := construct.BuildMonarchService(rs, rs.Namespace)
		svc.Spec.Selector = fresh.Spec.Selector
		svc.Spec.Ports = fresh.Spec.Ports
		svc.Labels = fresh.Labels
		return nil
	}); err != nil {
		return workflow.Failed(xerrors.Errorf("failed to create/update Monarch Service %s: %w", svc.Name, err))
	}

	// Hash the ConfigMap content so pod template annotation changes trigger a rolling restart
	configHash := monarchConfigHash(cm.Data)

	// 3. Create or update Monarch Deployment
	dep := construct.BuildMonarchDeployment(rs, rs.Namespace)
	if _, err := controllerutil.CreateOrUpdate(ctx, reconciler.client, dep, func() error {
		fresh := construct.BuildMonarchDeployment(rs, rs.Namespace)
		dep.Spec = fresh.Spec
		dep.Labels = fresh.Labels
		if dep.Spec.Template.Annotations == nil {
			dep.Spec.Template.Annotations = map[string]string{}
		}
		dep.Spec.Template.Annotations["checksum/config"] = configHash
		return nil
	}); err != nil {
		apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
			Type:    conditionType,
			Status:  metav1.ConditionFalse,
			Reason:  mdbv1.ReasonMonarchDeploymentFailed,
			Message: fmt.Sprintf("Failed to create/update Monarch Deployment: %v", err),
		})
		return workflow.Failed(xerrors.Errorf("failed to create/update Monarch Deployment %s: %w", dep.Name, err))
	}

	// 4. Wait for at least one Deployment replica to be ready before pushing automation config.
	// This ensures agents only see InjectorInstances when the health endpoint is already serving.
	if err := waitForMonarchDeploymentReady(ctx, reconciler.client, dep, 120*time.Second); err != nil {
		apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
			Type:    conditionType,
			Status:  metav1.ConditionFalse,
			Reason:  mdbv1.ReasonMonarchDeploymentPending,
			Message: fmt.Sprintf("Waiting for Deployment: %v", err),
		})
		return workflow.Pending("Waiting for Monarch %s Deployment to be ready: %v", monarchRole, err)
	}

	// 5. Build automation config with InjectorInstances pointing to the now-healthy Service
	serviceDNS := construct.GetMonarchServiceDNS(rs.Name, monarchRole, rs.Namespace)
	memberSvcName := rs.ServiceName()
	memberHostnames := make([]string, rs.Spec.Members)
	for i := 0; i < rs.Spec.Members; i++ {
		memberHostnames[i] = fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", rs.Name, i, memberSvcName, rs.Namespace)
	}
	mc, err := om.BuildMaintainedMonarchComponents(rs, rs.Name, awsKeyId, awsSecret, memberHostnames, serviceDNS)
	if err != nil {
		return workflow.Failed(xerrors.Errorf("failed to build Monarch automation config: %w", err))
	}

	// 7. Push automation config — agents will see InjectorInstances pointing to a healthy service
	err = conn.ReadUpdateDeployment(
		func(d om.Deployment) error {
			d.SetMaintainedMonarchComponents(mc)
			return nil
		},
		log,
	)
	if err != nil {
		apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
			Type:    conditionType,
			Status:  metav1.ConditionFalse,
			Reason:  mdbv1.ReasonMonarchDeploymentFailed,
			Message: fmt.Sprintf("Failed to push automation config: %v", err),
		})
		return workflow.Failed(xerrors.Errorf("failed to update automation config with Monarch components: %w", err))
	}

	// 8. Update condition to reflect final ready state
	apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
		Type:    conditionType,
		Status:  metav1.ConditionTrue,
		Reason:  mdbv1.ReasonMonarchDeploymentReady,
		Message: fmt.Sprintf("%d/%d pods ready (role=%s)", dep.Status.ReadyReplicas, ptr.Deref(dep.Spec.Replicas, 1), role),
	})
	log.Infof("Reconciled Monarch %s with %d replicas", monarchRole, construct.DefaultMonarchReplicas)
	return workflow.OK()
}

// cleanupMonarchResources deletes Monarch Deployment, Service, and ConfigMap when spec.monarch is removed.
func (r *ReplicaSetReconcilerHelper) cleanupMonarchResources(ctx context.Context) workflow.Status {
	rs := r.resource
	reconciler := r.reconciler
	log := r.log

	// Delete both shipper and injector resources since we don't know which role was previously active.
	// OwnerReferences should handle this, but explicit cleanup ensures resources are removed
	// even if the owner reference chain is broken.
	for _, role := range []string{"shipper", "injector"} {
		// Delete Deployment
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      construct.MonarchDeploymentName(rs.Name, role),
				Namespace: rs.Namespace,
			},
		}
		if err := reconciler.client.Delete(ctx, dep); err != nil && !errors.IsNotFound(err) {
			return workflow.Failed(xerrors.Errorf("failed to delete Monarch %s Deployment: %w", role, err))
		}

		// Delete Service
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      construct.MonarchServiceName(rs.Name, role),
				Namespace: rs.Namespace,
			},
		}
		if err := reconciler.client.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
			return workflow.Failed(xerrors.Errorf("failed to delete Monarch %s Service: %w", role, err))
		}

		// Delete ConfigMap
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      construct.MonarchConfigMapName(rs.Name, role),
				Namespace: rs.Namespace,
			},
		}
		if err := reconciler.client.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
			return workflow.Failed(xerrors.Errorf("failed to delete Monarch %s ConfigMap: %w", role, err))
		}
	}

	log.Info("Cleaned up Monarch K8s resources")
	return workflow.OK()
}

// waitForMonarchDeploymentReady polls until at least one Deployment replica is ready.
func waitForMonarchDeploymentReady(ctx context.Context, c client.Client, dep *appsv1.Deployment, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 3*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		if err := c.Get(ctx, types.NamespacedName{Name: dep.Name, Namespace: dep.Namespace}, dep); err != nil {
			return false, err
		}
		return dep.Status.ReadyReplicas > 0, nil
	})
}

// detectMonarchRoleChange checks if the Monarch role has changed from the last achieved spec.
// Returns the previous role if a change is detected, or nil if no change (or first deployment).
func (r *ReplicaSetReconcilerHelper) detectMonarchRoleChange() *mdbv1.MonarchRole {
	rs := r.resource
	lastSpec := r.deploymentState.LastAchievedSpec

	// No Monarch spec in current or last - not a role change
	if rs.Spec.Monarch == nil {
		return nil
	}

	// First Monarch deployment - not a role change, just initial setup
	if lastSpec == nil || lastSpec.Monarch == nil {
		return nil
	}

	// Compare current role with last achieved role
	if lastSpec.Monarch.Role != rs.Spec.Monarch.Role {
		return &lastSpec.Monarch.Role
	}

	return nil
}

// isInitialMonarchSetup returns true if Monarch is being added for the first time.
// This is derived from lastAchievedSpec (passive record of history), not tracked state.
// Used to skip agent waiting when agents can't reach goal without Monarch infra.
func isInitialMonarchSetup(rs *mdbv1.MongoDB, lastSpec *mdbv1.MongoDbSpec) bool {
	if rs.Spec.Monarch == nil {
		return false
	}
	return lastSpec == nil || lastSpec.Monarch == nil
}

// AnnotationFailoverS3ETag stores the S3 ETag for CAS operations during failover.
const AnnotationFailoverS3ETag = "mongodb.com/failover-s3-etag"

// reconcileMonarchFailover handles the failover state machine when a role change is detected.
// It coordinates with the agent via S3: writes PromoteStandby, polls for StandbyReadyToPromote,
// swaps infrastructure (delete old deployment, create new), then writes Active state.
//
// State transitions (operator writes):
//   - Standby → Active: Write PromoteStandby → Poll for StandbyReadyToPromote → Swap infra → Write Active
//   - Active → Standby: Stop shipper → Create injector → Write Standby
func (r *ReplicaSetReconcilerHelper) reconcileMonarchFailover(ctx context.Context, previousRole mdbv1.MonarchRole) workflow.Status {
	rs := r.resource
	log := r.log
	newRole := rs.Spec.Monarch.Role

	log.Infow("Monarch role change detected, initiating failover",
		"previousRole", previousRole,
		"newRole", newRole,
	)

	// Initialize failover state if not already tracking
	if rs.Status.FailoverPhase == "" || rs.Status.FailoverPhase == mdbv1.FailoverPhaseIdle || rs.Status.FailoverPhase == mdbv1.FailoverPhaseComplete {
		rs.Status.FailoverPhase = mdbv1.FailoverPhaseWaitingForAgent

		// Set FailoverInProgress condition (contains timestamp and message)
		apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
			Type:    mdbv1.ConditionFailoverInProgress,
			Status:  metav1.ConditionTrue,
			Reason:  mdbv1.ReasonFailoverStarted,
			Message: fmt.Sprintf("Failover from %s to %s started", previousRole, newRole),
		})
	}

	// Handle based on transition direction
	if newRole == mdbv1.MonarchRoleActive {
		return r.promoteToActive(ctx)
	}
	return r.demoteToStandby(ctx)
}

// promoteToActive handles the standby → active promotion workflow.
// Phase 1: Write PromoteStandby to S3 (triggers agent)
// Phase 2: Poll S3 for StandbyReadyToPromote (agent completed RS reconfig)
// Phase 3: Swap infrastructure (delete injector, create shipper)
// Phase 4: Write Active to S3, complete failover
func (r *ReplicaSetReconcilerHelper) promoteToActive(ctx context.Context) workflow.Status {
	rs := r.resource
	log := r.log

	log.Infow("Promotion to active", "phase", rs.Status.FailoverPhase)

	// Create DR state client
	drClient, err := r.createDRStateClient(ctx)
	if err != nil {
		r.setFailoverFailed("Failed to create DR state client: %v", err)
		return workflow.Failed(xerrors.Errorf("failed to create DR state client: %w", err))
	}

	switch rs.Status.FailoverPhase {
	case mdbv1.FailoverPhaseWaitingForAgent:
		// Check if we've already written PromoteStandby (indicated by annotation)
		if rs.Annotations[AnnotationFailoverS3ETag] == "" {
			// Phase 1: Write PromoteStandby to S3 to trigger agent
			log.Info("Writing PromoteStandby state to S3")

			clusterID := fmt.Sprintf("%s/%s", rs.Namespace, rs.Name)
			result, err := drClient.TransitionTo(ctx, drstate.StatePromoteStandby, clusterID)
			if err != nil {
				if err == drstate.ErrCASConflict {
					log.Warn("CAS conflict writing PromoteStandby, will retry")
					return workflow.Pending("CAS conflict writing PromoteStandby, retrying")
				}
				r.setFailoverFailed("Failed to write PromoteStandby to S3: %v", err)
				return workflow.Failed(xerrors.Errorf("failed to write PromoteStandby to S3: %w", err))
			}

			// Store the ETag in annotation for CAS operations
			if rs.Annotations == nil {
				rs.Annotations = make(map[string]string)
			}
			rs.Annotations[AnnotationFailoverS3ETag] = result.ETag

			apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
				Type:    mdbv1.ConditionFailoverInProgress,
				Status:  metav1.ConditionTrue,
				Reason:  mdbv1.ReasonFailoverWaitingForAgent,
				Message: "Wrote PromoteStandby to S3, waiting for agent to complete RS reconfiguration",
			})

			log.Info("Wrote PromoteStandby to S3, waiting for agent response")
			return workflow.Pending("Waiting for agent to complete RS reconfiguration")
		}

		// Phase 2: Poll S3 for StandbyReadyToPromote
		log.Info("Polling S3 for StandbyReadyToPromote state")

		currentState, err := drClient.Read(ctx)
		if err != nil {
			log.Warnw("Failed to read DR state from S3", "error", err)
			return workflow.Pending("Failed to read DR state, retrying")
		}

		if currentState == nil {
			log.Warn("DR state file not found in S3")
			return workflow.Pending("DR state file not found, retrying")
		}

		log.Infow("Current DR state", "state", currentState.State)

		if currentState.State == drstate.StateStandbyReadyToPromote {
			// Agent has completed RS reconfiguration, proceed to infrastructure swap
			log.Info("Agent completed RS reconfiguration, proceeding to infrastructure swap")
			rs.Status.FailoverPhase = mdbv1.FailoverPhaseSwappingInfrastructure

			apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
				Type:    mdbv1.ConditionFailoverInProgress,
				Status:  metav1.ConditionTrue,
				Reason:  mdbv1.ReasonFailoverSwappingInfra,
				Message: "Agent completed RS reconfiguration, swapping infrastructure",
			})
			// Fall through to SwappingInfrastructure phase
		} else {
			return workflow.Pending("Waiting for agent to complete RS reconfiguration (current state: %s)", currentState.State)
		}
		fallthrough

	case mdbv1.FailoverPhaseSwappingInfrastructure:
		// Phase 3: Swap infrastructure (delete injector, create shipper)
		log.Info("Swapping infrastructure from injector to shipper")

		if status := r.deleteMonarchResourcesForRole(ctx, "injector"); !status.IsOK() {
			r.setFailoverFailed("Failed to delete injector: %v", status)
			return status
		}

		log.Info("Deleted injector resources")

		// Phase 4: Write Active state to S3
		log.Info("Writing Active state to S3")
		clusterID := fmt.Sprintf("%s/%s", rs.Namespace, rs.Name)
		_, err := drClient.TransitionTo(ctx, drstate.StateActive, clusterID)
		if err != nil {
			if err == drstate.ErrCASConflict {
				log.Warn("CAS conflict writing Active state, will retry")
				return workflow.Pending("CAS conflict writing Active state, retrying")
			}
			// Log but don't fail - the infrastructure swap is complete
			log.Warnw("Failed to write Active state to S3, continuing anyway", "error", err)
		}

		// Mark failover as complete
		r.setFailoverComplete("Successfully promoted to active")

		// Remove old InjectorReady condition since we're now active
		apimeta.RemoveStatusCondition(&rs.Status.Conditions, mdbv1.ConditionInjectorReady)

		log.Info("Promotion to active complete")
		return workflow.OK()

	default:
		log.Warnw("Unexpected failover phase in promoteToActive", "phase", rs.Status.FailoverPhase)
		rs.Status.FailoverPhase = mdbv1.FailoverPhaseWaitingForAgent
		return workflow.Pending("Resetting failover state")
	}
}

// demoteToStandby handles the active → standby demotion workflow.
// Phase 1: Delete shipper resources
// Phase 2: Create injector resources (will be done by reconcileMonarch)
// Phase 3: Write Standby to S3
func (r *ReplicaSetReconcilerHelper) demoteToStandby(ctx context.Context) workflow.Status {
	rs := r.resource
	log := r.log

	log.Infow("Demotion to standby", "phase", rs.Status.FailoverPhase)

	// Create DR state client
	drClient, err := r.createDRStateClient(ctx)
	if err != nil {
		r.setFailoverFailed("Failed to create DR state client: %v", err)
		return workflow.Failed(xerrors.Errorf("failed to create DR state client: %w", err))
	}

	switch rs.Status.FailoverPhase {
	case mdbv1.FailoverPhaseWaitingForAgent:
		// For demotion, we go directly to infrastructure swap (no agent coordination needed)
		rs.Status.FailoverPhase = mdbv1.FailoverPhaseSwappingInfrastructure
		fallthrough

	case mdbv1.FailoverPhaseSwappingInfrastructure:
		// Phase 1: Delete shipper resources
		log.Info("Deleting shipper resources")

		if status := r.deleteMonarchResourcesForRole(ctx, "shipper"); !status.IsOK() {
			r.setFailoverFailed("Failed to delete shipper: %v", status)
			return status
		}

		log.Info("Deleted shipper resources")

		// Phase 2: Write Standby state to S3
		log.Info("Writing Standby state to S3")
		clusterID := fmt.Sprintf("%s/%s", rs.Namespace, rs.Name)
		_, err := drClient.TransitionTo(ctx, drstate.StateStandby, clusterID)
		if err != nil {
			if err == drstate.ErrCASConflict {
				log.Warn("CAS conflict writing Standby state, will retry")
				return workflow.Pending("CAS conflict writing Standby state, retrying")
			}
			// Log but don't fail - the infrastructure swap is complete
			log.Warnw("Failed to write Standby state to S3, continuing anyway", "error", err)
		}

		// Mark failover as complete
		r.setFailoverComplete("Successfully demoted to standby")

		// Remove old ShipperReady condition since we're now standby
		apimeta.RemoveStatusCondition(&rs.Status.Conditions, mdbv1.ConditionShipperReady)

		log.Info("Demotion to standby complete")
		return workflow.OK()

	default:
		log.Warnw("Unexpected failover phase in demoteToStandby", "phase", rs.Status.FailoverPhase)
		rs.Status.FailoverPhase = mdbv1.FailoverPhaseSwappingInfrastructure
		return workflow.Pending("Resetting failover state")
	}
}

// setFailoverFailed marks the failover as failed with the given message.
func (r *ReplicaSetReconcilerHelper) setFailoverFailed(format string, args ...interface{}) {
	rs := r.resource
	rs.Status.FailoverPhase = mdbv1.FailoverPhaseFailed
	delete(rs.Annotations, AnnotationFailoverS3ETag)

	apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
		Type:    mdbv1.ConditionFailoverInProgress,
		Status:  metav1.ConditionFalse,
		Reason:  mdbv1.ReasonFailoverFailed,
		Message: fmt.Sprintf(format, args...),
	})
}

// setFailoverComplete marks the failover as complete with the given message.
func (r *ReplicaSetReconcilerHelper) setFailoverComplete(message string) {
	rs := r.resource
	rs.Status.FailoverPhase = mdbv1.FailoverPhaseComplete
	delete(rs.Annotations, AnnotationFailoverS3ETag)

	apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
		Type:    mdbv1.ConditionFailoverInProgress,
		Status:  metav1.ConditionFalse,
		Reason:  mdbv1.ReasonFailoverSucceeded,
		Message: message,
	})

	apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
		Type:    mdbv1.ConditionFailoverComplete,
		Status:  metav1.ConditionTrue,
		Reason:  mdbv1.ReasonFailoverSucceeded,
		Message: message,
	})
}

// reconcileMonarchS3State reads the S3 DR state on every reconcile and handles unplanned failover.
// This addresses the case where an external tool (CLI) writes to S3 directly without updating the CR.
// Per K8s conventions:
// - S3 drives infrastructure (swap shipper/injector based on S3 state)
// - status.Monarch reflects observed S3 state
// - SpecOutOfSync condition warns when CR differs from S3
// - CR spec remains unchanged (user's declared intent is preserved)
//
// IMPORTANT: Only standby clusters monitor the S3 DR state file. Active clusters only have a shipper
// (no injector config), so the agent doesn't monitor S3 state. For unplanned failover, only the
// standby cluster needs to detect when an external tool writes PromoteStandby to S3.
func (r *ReplicaSetReconcilerHelper) reconcileMonarchS3State(ctx context.Context) workflow.Status {
	rs := r.resource
	log := r.log

	// Active clusters don't monitor S3 state - they only have shipper, no injector config.
	// Only standby clusters read the S3 DR state file to detect unplanned failover.
	if rs.Spec.Monarch.Role == mdbv1.MonarchRoleActive {
		log.Debug("Active cluster, skipping S3 state check (only standby monitors S3)")
		return workflow.OK()
	}

	// Skip S3 state check if a failover is already in progress (triggered by CR change).
	// The failover state machine handles S3 coordination during planned failover.
	if rs.Status.FailoverPhase != "" &&
		rs.Status.FailoverPhase != mdbv1.FailoverPhaseIdle &&
		rs.Status.FailoverPhase != mdbv1.FailoverPhaseComplete {
		log.Debug("Failover in progress, skipping S3 state check")
		return workflow.OK()
	}

	// Create DR state client
	drClient, err := r.createDRStateClient(ctx)
	if err != nil {
		// Log but don't fail - S3 state reading is best-effort for unplanned failover detection.
		// The cluster should continue operating even if S3 is unreachable.
		log.Warnw("Failed to create DR state client, skipping S3 state check", "error", err)
		return workflow.OK()
	}

	// Read S3 DR state
	s3State, err := drClient.Read(ctx)
	if err != nil {
		log.Warnw("Failed to read DR state from S3", "error", err)
		return workflow.OK()
	}

	// Update status.Monarch with observed S3 state
	now := metav1.Now()
	if rs.Status.Monarch == nil {
		rs.Status.Monarch = &mdbv1.MonarchStatus{}
	}
	if s3State != nil {
		rs.Status.Monarch.ObservedS3State = string(s3State.State)
		rs.Status.Monarch.ObservedS3StateTime = &now
	}

	// If S3 state file doesn't exist yet, nothing to do
	if s3State == nil {
		log.Debug("S3 DR state file not found, skipping mismatch check")
		apimeta.RemoveStatusCondition(&rs.Status.Conditions, mdbv1.ConditionSpecOutOfSync)
		return workflow.OK()
	}

	// Derive expected role from S3 state
	// Active or StandbyReadyToPromote → should be active (have shipper)
	// Standby or PromoteStandby → should be standby (have injector)
	s3ExpectsActive := s3State.State == drstate.StateActive || s3State.State == drstate.StateStandbyReadyToPromote
	crSpecActive := rs.Spec.Monarch.Role == mdbv1.MonarchRoleActive

	// Check for spec mismatch
	if s3ExpectsActive != crSpecActive {
		log.Infow("S3 state differs from CR spec",
			"s3State", s3State.State,
			"crRole", rs.Spec.Monarch.Role,
			"s3ExpectsActive", s3ExpectsActive,
		)

		// Handle unplanned failover: S3 shows promotion completed but CR still says standby
		if s3ExpectsActive && !crSpecActive {
			return r.handleUnplannedPromotion(ctx, s3State.State)
		}

		// S3 shows standby but CR says active (demotion scenario)
		// OUT OF SCOPE: Demotion is not designed yet - just set warning condition
		apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
			Type:    mdbv1.ConditionSpecOutOfSync,
			Status:  metav1.ConditionTrue,
			Reason:  "S3StateMismatch",
			Message: fmt.Sprintf("CR spec.monarch.role is '%s' but S3 DR state is '%s'. Demotion not yet supported.", rs.Spec.Monarch.Role, s3State.State),
		})
		return workflow.OK()
	}

	// No mismatch - remove the condition if it was previously set
	apimeta.RemoveStatusCondition(&rs.Status.Conditions, mdbv1.ConditionSpecOutOfSync)
	return workflow.OK()
}

// handleUnplannedPromotion handles the case where S3 shows the cluster should be active
// but the CR spec still says standby. This happens during unplanned failover when an
// external tool (CLI) writes to S3 directly.
func (r *ReplicaSetReconcilerHelper) handleUnplannedPromotion(ctx context.Context, s3State drstate.State) workflow.Status {
	rs := r.resource
	log := r.log

	log.Infow("Handling unplanned promotion", "s3State", s3State)

	// Check current infrastructure state
	hasInjector := r.monarchDeploymentExists(ctx, "injector")
	hasShipper := r.monarchDeploymentExists(ctx, "shipper")

	if s3State == drstate.StateStandbyReadyToPromote {
		// Agent has completed RS reconfiguration, we need to swap infrastructure
		log.Info("Agent completed RS reconfiguration (unplanned), swapping infrastructure")

		// Delete injector if present
		if hasInjector {
			if status := r.deleteMonarchResourcesForRole(ctx, "injector"); !status.IsOK() {
				return status
			}
			log.Info("Deleted injector resources (unplanned promotion)")
		}

		// Write Active state to S3 to complete the promotion
		drClient, err := r.createDRStateClient(ctx)
		if err != nil {
			log.Warnw("Failed to create DR state client for S3 write", "error", err)
		} else {
			clusterID := fmt.Sprintf("%s/%s", rs.Namespace, rs.Name)
			if _, err := drClient.TransitionTo(ctx, drstate.StateActive, clusterID); err != nil {
				if err == drstate.ErrCASConflict {
					log.Warn("CAS conflict writing Active state (unplanned), will retry")
					return workflow.Pending("CAS conflict writing Active state, retrying")
				}
				log.Warnw("Failed to write Active state to S3 (unplanned)", "error", err)
			}
		}
	}

	// S3 state is Active - infrastructure should be shipper
	if s3State == drstate.StateActive && hasInjector && !hasShipper {
		// Injector still exists but S3 says Active - delete injector
		log.Info("S3 shows Active but injector exists, deleting injector (unplanned)")
		if status := r.deleteMonarchResourcesForRole(ctx, "injector"); !status.IsOK() {
			return status
		}
	}

	// Set SpecOutOfSync condition to warn user
	apimeta.SetStatusCondition(&rs.Status.Conditions, metav1.Condition{
		Type:   mdbv1.ConditionSpecOutOfSync,
		Status: metav1.ConditionTrue,
		Reason: "UnplannedPromotion",
		Message: fmt.Sprintf(
			"Unplanned promotion detected: S3 DR state is '%s' but CR spec.monarch.role is 'standby'. "+
				"Update the CR to role: active to acknowledge.", s3State),
	})

	// Update status to reflect the actual state
	now := metav1.Now()
	rs.Status.Monarch.ObservedS3State = string(s3State)
	rs.Status.Monarch.ObservedS3StateTime = &now

	// Remove InjectorReady since we're transitioning to active
	apimeta.RemoveStatusCondition(&rs.Status.Conditions, mdbv1.ConditionInjectorReady)

	// Continue with normal reconciliation - shipper will be created because
	// we look at S3 state (Active) not CR spec (standby).
	// The SpecOutOfSync condition tells the user to update their CR.
	log.Info("Unplanned promotion handled, shipper will be created by normal reconciliation")

	// Return pending to trigger another reconcile that creates the shipper
	return workflow.Pending("Unplanned promotion: infrastructure swap in progress")
}

// monarchDeploymentExists checks if a Monarch Deployment exists for the given role.
func (r *ReplicaSetReconcilerHelper) monarchDeploymentExists(ctx context.Context, role string) bool {
	rs := r.resource
	reconciler := r.reconciler

	dep := &appsv1.Deployment{}
	err := reconciler.client.Get(ctx, types.NamespacedName{
		Name:      construct.MonarchDeploymentName(rs.Name, role),
		Namespace: rs.Namespace,
	}, dep)
	return err == nil
}

// createDRStateClient creates a DR state client for S3 coordination during failover.
func (r *ReplicaSetReconcilerHelper) createDRStateClient(ctx context.Context) (*drstate.Client, error) {
	rs := r.resource
	reconciler := r.reconciler
	s3Cfg := rs.Spec.Monarch.S3

	// Read AWS credentials from the secret
	credSecret, err := reconciler.client.GetSecret(ctx, kube.ObjectKey(rs.Namespace, s3Cfg.CredentialsSecretRef.Name))
	if err != nil {
		return nil, xerrors.Errorf("failed to read Monarch credentials secret %s: %w", s3Cfg.CredentialsSecretRef.Name, err)
	}

	awsKeyId := string(credSecret.Data["awsAccessKeyId"])
	awsSecret := string(credSecret.Data["awsSecretAccessKey"])

	if awsKeyId == "" || awsSecret == "" {
		return nil, xerrors.Errorf("Monarch credentials secret %s missing awsAccessKeyId or awsSecretAccessKey", s3Cfg.CredentialsSecretRef.Name)
	}

	cfg := drstate.ClientConfig{
		BucketName:      s3Cfg.Bucket,
		Region:          s3Cfg.Region,
		ClusterPrefix:   s3Cfg.GetPrefix(rs.Name),
		Endpoint:        s3Cfg.Endpoint,
		PathStyleAccess: s3Cfg.PathStyle,
		AccessKeyID:     awsKeyId,
		SecretAccessKey: awsSecret,
	}

	return drstate.NewClient(ctx, cfg)
}

// deleteMonarchResourcesForRole deletes Deployment, Service, and ConfigMap for the specified role.
func (r *ReplicaSetReconcilerHelper) deleteMonarchResourcesForRole(ctx context.Context, role string) workflow.Status {
	rs := r.resource
	reconciler := r.reconciler
	log := r.log

	log.Infow("Deleting Monarch resources for role", "role", role)

	// Delete Deployment
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      construct.MonarchDeploymentName(rs.Name, role),
			Namespace: rs.Namespace,
		},
	}
	if err := reconciler.client.Delete(ctx, dep); err != nil && !errors.IsNotFound(err) {
		return workflow.Failed(xerrors.Errorf("failed to delete Monarch %s Deployment: %w", role, err))
	}

	// Delete Service
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      construct.MonarchServiceName(rs.Name, role),
			Namespace: rs.Namespace,
		},
	}
	if err := reconciler.client.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
		return workflow.Failed(xerrors.Errorf("failed to delete Monarch %s Service: %w", role, err))
	}

	// Delete ConfigMap
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      construct.MonarchConfigMapName(rs.Name, role),
			Namespace: rs.Namespace,
		},
	}
	if err := reconciler.client.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
		return workflow.Failed(xerrors.Errorf("failed to delete Monarch %s ConfigMap: %w", role, err))
	}

	log.Infow("Deleted Monarch resources for role", "role", role)
	return workflow.OK()
}

// buildMongoDBConnectionString builds a MongoDB connection string for the replica set.
func buildMongoDBConnectionString(rs *mdbv1.MongoDB) string {
	svcName := rs.ServiceName()
	namespace := rs.Namespace
	members := rs.Spec.Members
	port := 27017 // default MongoDB port

	// Build host list: <rs>-0.<svc>.<ns>.svc.cluster.local:27017,...
	hosts := ""
	for i := 0; i < members; i++ {
		if i > 0 {
			hosts += ","
		}
		hosts += fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d", rs.Name, i, svcName, namespace, port)
	}

	return fmt.Sprintf("mongodb://%s/?replicaSet=%s", hosts, rs.Name)
}

func (r *ReplicaSetReconcilerHelper) reconcileStatefulSet(ctx context.Context, conn om.Connection, projectConfig mdbv1.ProjectConfig, deploymentOptions deploymentOptionsRS) workflow.Status {
	rs := r.resource
	reconciler := r.reconciler
	log := r.log

	certConfigurator := certs.ReplicaSetX509CertConfigurator{MongoDB: rs, SecretClient: reconciler.SecretClient}
	status := reconciler.ensureX509SecretAndCheckTLSType(ctx, certConfigurator, deploymentOptions.currentAgentAuthMode, log)
	if !status.IsOK() {
		return status
	}

	status = certs.EnsureSSLCertsForStatefulSet(ctx, reconciler.SecretClient, reconciler.SecretClient, *rs.Spec.Security, certs.ReplicaSetConfig(*rs), log)
	if !status.IsOK() {
		return status
	}

	// Build the replica set config
	rsConfig, err := r.buildStatefulSetOptions(ctx, conn, projectConfig, deploymentOptions)
	if err != nil {
		return workflow.Failed(xerrors.Errorf("failed to build StatefulSet options: %w", err))
	}

	sts := construct.DatabaseStatefulSet(*rs, rsConfig, log)

	// Handle PVC resize if needed
	if workflowStatus := r.handlePVCResize(ctx, &sts); !workflowStatus.IsOK() {
		return workflowStatus
	}

	// Create or update the StatefulSet in Kubernetes
	mutatedSts, err := create.DatabaseInKubernetes(ctx, reconciler.client, *rs, sts, rsConfig, log)
	if err != nil {
		return workflow.Failed(xerrors.Errorf("failed to create/update (Kubernetes reconciliation phase): %w", err))
	}

	// Check StatefulSet status
	expectedGeneration := mutatedSts.GetGeneration()
	if status := statefulset.GetStatefulSetStatus(ctx, rs.Namespace, rs.Name, expectedGeneration, reconciler.client); !status.IsOK() {
		return status
	}

	log.Info("Updated StatefulSet for replica set")
	return workflow.OK()
}

func (r *ReplicaSetReconcilerHelper) handlePVCResize(ctx context.Context, sts *appsv1.StatefulSet) workflow.Status {
	workflowStatus := create.HandlePVCResize(ctx, r.reconciler.client, sts, r.log)
	if !workflowStatus.IsOK() {
		return workflowStatus
	}

	if workflow.ContainsPVCOption(workflowStatus.StatusOptions()) {
		if _, err := r.reconciler.updateStatus(ctx, r.resource, workflow.Pending(""), r.log, workflowStatus.StatusOptions()...); err != nil {
			return workflow.Failed(xerrors.Errorf("error updating status: %w", err))
		}
	}
	return workflow.OK()
}

// buildStatefulSetOptions creates the options needed for constructing the StatefulSet
func (r *ReplicaSetReconcilerHelper) buildStatefulSetOptions(ctx context.Context, conn om.Connection, projectConfig mdbv1.ProjectConfig, deploymentOptions deploymentOptionsRS) (func(mdb mdbv1.MongoDB) construct.DatabaseStatefulSetOptions, error) {
	rs := r.resource
	reconciler := r.reconciler
	log := r.log

	rsCertsConfig := certs.ReplicaSetConfig(*rs)

	var vaultConfig vault.VaultConfiguration
	var databaseSecretPath string
	if reconciler.VaultClient != nil {
		vaultConfig = reconciler.VaultClient.VaultConfig
		databaseSecretPath = reconciler.VaultClient.DatabaseSecretPath()
	}

	// Determine automation agent version for static architecture
	var automationAgentVersion string
	if architectures.IsRunningStaticArchitecture(rs.Annotations) {
		// In case the Agent *is* overridden, its version will be merged into the StatefulSet. The merging process
		// happens after creating the StatefulSet definition.
		if !rs.IsAgentImageOverridden() {
			var err error
			automationAgentVersion, err = reconciler.getAgentVersion(conn, conn.OpsManagerVersion().VersionString, false, log)
			if err != nil {
				return nil, xerrors.Errorf("impossible to get agent version, please override the agent image by providing a pod template: %w", err)
			}
		}
	}

	tlsCertHash := enterprisepem.ReadHashFromSecret(ctx, reconciler.SecretClient, rs.Namespace, rsCertsConfig.CertSecretName, databaseSecretPath, log)
	internalClusterCertHash := enterprisepem.ReadHashFromSecret(ctx, reconciler.SecretClient, rs.Namespace, rsCertsConfig.InternalClusterSecretName, databaseSecretPath, log)

	rsConfig := construct.ReplicaSetOptions(
		PodEnvVars(newPodVars(conn, projectConfig, rs.Spec.LogLevel)),
		CurrentAgentAuthMechanism(deploymentOptions.currentAgentAuthMode),
		CertificateHash(tlsCertHash),
		AgentCertHash(deploymentOptions.agentCertHash),
		InternalClusterHash(internalClusterCertHash),
		PrometheusTLSCertHash(deploymentOptions.prometheusCertHash),
		WithVaultConfig(vaultConfig),
		WithLabels(rs.Labels),
		WithAdditionalMongodConfig(rs.Spec.GetAdditionalMongodConfig()),
		WithInitDatabaseNonStaticImage(images.ContainerImage(reconciler.imageUrls, util.InitDatabaseImageUrlEnv, reconciler.initDatabaseNonStaticImageVersion)),
		WithDatabaseNonStaticImage(images.ContainerImage(reconciler.imageUrls, util.NonStaticDatabaseEnterpriseImage, reconciler.databaseNonStaticImageVersion)),
		WithAgentImage(images.ContainerImage(reconciler.imageUrls, architectures.MdbAgentImageRepo, automationAgentVersion)),
		WithMongodbImage(images.GetOfficialImage(reconciler.imageUrls, rs.Spec.Version, rs.GetAnnotations())),
		WithAgentDebug(reconciler.agentDebug),
		WithAgentDebugImage(reconciler.agentDebugImage),
	)

	return rsConfig, nil
}

// AddReplicaSetController creates a new MongoDbReplicaset Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func AddReplicaSetController(ctx context.Context, mgr manager.Manager, imageUrls images.ImageUrls, initDatabaseNonStaticImageVersion, databaseNonStaticImageVersion string, forceEnterprise, enableClusterMongoDBRoles, agentDebug bool, agentDebugImage string) error {
	// Create a new controller
	reconciler := newReplicaSetReconciler(ctx, mgr.GetClient(), imageUrls, initDatabaseNonStaticImageVersion, databaseNonStaticImageVersion, forceEnterprise, enableClusterMongoDBRoles, agentDebug, agentDebugImage, om.NewOpsManagerConnection)
	c, err := controller.New(util.MongoDbReplicaSetController, mgr, controller.Options{Reconciler: reconciler, MaxConcurrentReconciles: env.ReadIntOrDefault(util.MaxConcurrentReconcilesEnv, 1)}) // nolint:forbidigo
	if err != nil {
		return err
	}

	// watch for changes to replica set MongoDB resources
	eventHandler := ResourceEventHandler{deleter: reconciler}
	// Watch for changes to primary resource MongoDbReplicaSet
	err = c.Watch(source.Kind[client.Object](mgr.GetCache(), &mdbv1.MongoDB{}, &eventHandler, watch.PredicatesForMongoDB(mdbv1.ReplicaSet)))
	if err != nil {
		return err
	}

	err = c.Watch(source.Channel(OmUpdateChannel, &handler.EnqueueRequestForObject{}, source.WithPredicates[client.Object, reconcile.Request](watch.PredicatesForMongoDB(mdbv1.ReplicaSet))))
	if err != nil {
		return xerrors.Errorf("not able to setup OmUpdateChannel to listent to update events from OM: %s", err)
	}

	err = c.Watch(
		source.Kind[client.Object](mgr.GetCache(), &appsv1.StatefulSet{},
			handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &mdbv1.MongoDB{}, handler.OnlyControllerOwner()),
			watch.PredicatesForStatefulSet()))
	if err != nil {
		return err
	}

	// Watch Deployments for Monarch components
	err = c.Watch(
		source.Kind[client.Object](mgr.GetCache(), &appsv1.Deployment{},
			handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &mdbv1.MongoDB{}, handler.OnlyControllerOwner())))
	if err != nil {
		return err
	}

	err = c.Watch(source.Kind[client.Object](mgr.GetCache(), &corev1.ConfigMap{},
		&watch.ResourcesHandler{ResourceType: watch.ConfigMap, ResourceWatcher: reconciler.resourceWatcher}))
	if err != nil {
		return err
	}

	err = c.Watch(source.Kind[client.Object](mgr.GetCache(), &corev1.Secret{},
		&watch.ResourcesHandler{ResourceType: watch.Secret, ResourceWatcher: reconciler.resourceWatcher}))
	if err != nil {
		return err
	}

	if enableClusterMongoDBRoles {
		err = c.Watch(source.Kind[client.Object](mgr.GetCache(), &rolev1.ClusterMongoDBRole{},
			&watch.ResourcesHandler{ResourceType: watch.ClusterMongoDBRole, ResourceWatcher: reconciler.resourceWatcher}))
		if err != nil {
			return err
		}
	}

	// if vault secret backend is enabled watch for Vault secret change and trigger reconcile
	if vault.IsVaultSecretBackend() {
		eventChannel := make(chan event.GenericEvent)
		go vaultwatcher.WatchSecretChangeForMDB(ctx, zap.S(), eventChannel, reconciler.client, reconciler.VaultClient, mdbv1.ReplicaSet)

		err = c.Watch(source.Channel[client.Object](eventChannel, &handler.EnqueueRequestForObject{}))
		if err != nil {
			zap.S().Errorf("Failed to watch for vault secret changes: %w", err)
		}
	}

	// Watch for MongoDBSearch resources that reference ReplicaSet MongoDB resources
	// Only enqueue reconciliation requests for ReplicaSet resources, not Standalone or ShardedCluster
	kubeClient := mgr.GetClient()
	err = c.Watch(source.Kind(mgr.GetCache(), &searchv1.MongoDBSearch{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, search *searchv1.MongoDBSearch) []reconcile.Request {
			sourceRef := search.GetMongoDBResourceRef()
			if sourceRef == nil {
				return []reconcile.Request{}
			}
			// Fetch the MongoDB resource to check its ResourceType
			mdb := &mdbv1.MongoDB{}
			if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: sourceRef.Namespace, Name: sourceRef.Name}, mdb); err != nil {
				// If we can't fetch the resource, don't enqueue (it might not exist or be a different type)
				return []reconcile.Request{}
			}
			// Only enqueue if this is a ReplicaSet resource
			if mdb.Spec.ResourceType != mdbv1.ReplicaSet {
				return []reconcile.Request{}
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: sourceRef.Namespace, Name: sourceRef.Name}}}
		})))
	if err != nil {
		return err
	}

	zap.S().Infof("Registered controller %s", util.MongoDbReplicaSetController)

	return nil
}

// updateOmDeploymentRs performs OM registration operation for the replicaset. So the changes will be finally propagated
// to automation agents in containers
func (r *ReplicaSetReconcilerHelper) updateOmDeploymentRs(ctx context.Context, conn om.Connection, membersNumberBefore int, tlsCertPath, internalClusterCertPath string, deploymentOptions deploymentOptionsRS, shouldMirrorKeyfileForMongot bool, isRecovering bool) workflow.Status {
	rs := r.resource
	log := r.log
	reconciler := r.reconciler
	log.Debug("Entering UpdateOMDeployments")
	// Only "concrete" RS members should be observed
	// - if scaling down, let's observe only members that will remain after scale-down operation
	// - if scaling up, observe only current members, because new ones might not exist yet
	replicasTarget := scale.ReplicasThisReconciliation(rs)
	err := agents.WaitForRsAgentsToRegisterByResource(rs, util_int.Min(membersNumberBefore, replicasTarget), conn, log)
	if err != nil && !isRecovering {
		return workflow.Failed(err)
	}

	caFilePath := fmt.Sprintf("%s/ca-pem", util.TLSCaMountPath)

	replicaSet := replicaset.BuildFromMongoDBWithReplicas(reconciler.imageUrls[mcoConstruct.MongodbImageEnv], reconciler.forceEnterprise, rs, replicasTarget, rs.CalculateFeatureCompatibilityVersion(), tlsCertPath)
	processNames := replicaSet.GetProcessNames()

	status, additionalReconciliationRequired := reconciler.updateOmAuthentication(ctx, conn, processNames, rs, deploymentOptions.agentCertPath, caFilePath, internalClusterCertPath, isRecovering, log)
	if !status.IsOK() && !isRecovering {
		return status
	}

	lastRsConfig, err := mdbv1.GetLastAdditionalMongodConfigByType(r.deploymentState.LastAchievedSpec, mdbv1.ReplicaSetConfig)
	if err != nil && !isRecovering {
		return workflow.Failed(err)
	}

	prometheusConfiguration := PrometheusConfiguration{
		prometheus:         rs.GetPrometheus(),
		conn:               conn,
		secretsClient:      reconciler.SecretClient,
		namespace:          rs.GetNamespace(),
		prometheusCertHash: deploymentOptions.prometheusCertHash,
	}

	err = conn.ReadUpdateDeployment(
		func(d om.Deployment) error {
			if shouldMirrorKeyfileForMongot {
				if err := r.mirrorKeyfileIntoSecretForMongot(ctx, d); err != nil {
					return err
				}
			}
			return ReconcileReplicaSetAC(ctx, d, rs.Spec.DbCommonSpec, lastRsConfig.ToMap(), rs.Name, replicaSet, caFilePath, internalClusterCertPath, &prometheusConfiguration, log)
		},
		log,
	)

	if err != nil && !isRecovering {
		return workflow.Failed(err)
	}

	// Skip waiting for agents during initial Monarch setup.
	// Agents can't reach goal state until injector/shipper Deployment exists,
	// which is created by reconcileMonarch (runs after this function).
	// On next reconcile, Monarch infra will exist and agents will reach goal.
	if isInitialMonarchSetup(rs, r.deploymentState.LastAchievedSpec) {
		log.Info("Skipping agent wait during initial Monarch setup - Deployment will be created next")
	} else {
		if err := om.WaitForReadyState(conn, processNames, isRecovering, log); err != nil {
			return workflow.Failed(err)
		}
	}

	reconcileResult, _ := ReconcileLogRotateSetting(conn, rs.Spec.Agent, log)
	if !reconcileResult.IsOK() {
		return reconcileResult
	}

	if additionalReconciliationRequired {
		return workflow.Pending("Performing multi stage reconciliation")
	}

	hostsBefore := getAllHostsForReplicas(rs, membersNumberBefore)
	hostsAfter := getAllHostsForReplicas(rs, scale.ReplicasThisReconciliation(rs))

	if err := host.CalculateDiffAndStopMonitoring(conn, hostsBefore, hostsAfter, log); err != nil && !isRecovering {
		return workflow.Failed(err)
	}

	if status := reconciler.ensureBackupConfigurationAndUpdateStatus(ctx, conn, rs, reconciler.SecretClient, log, 0); !status.IsOK() && !isRecovering {
		return status
	}

	log.Info("Updated Ops Manager for replica set")
	return workflow.OK()
}

func (r *ReplicaSetReconcilerHelper) OnDelete(ctx context.Context, obj runtime.Object, log *zap.SugaredLogger) error {
	rs := obj.(*mdbv1.MongoDB)

	if err := r.cleanOpsManagerState(ctx, rs, log); err != nil {
		return err
	}

	r.reconciler.resourceWatcher.RemoveDependentWatchedResources(rs.ObjectKey())

	return nil
}

func (r *ReplicaSetReconcilerHelper) cleanOpsManagerState(ctx context.Context, rs *mdbv1.MongoDB, log *zap.SugaredLogger) error {
	projectConfig, credsConfig, err := project.ReadConfigAndCredentials(ctx, r.reconciler.client, r.reconciler.SecretClient, rs, log)
	if err != nil {
		return err
	}

	log.Infow("Removing replica set from Ops Manager", "config", rs.Spec)
	conn, _, err := connection.PrepareOpsManagerConnection(ctx, r.reconciler.SecretClient, projectConfig, credsConfig, r.reconciler.omConnectionFactory, rs.Namespace, log)
	if err != nil {
		return err
	}

	processNames := make([]string, 0)
	err = conn.ReadUpdateDeployment(
		func(d om.Deployment) error {
			processNames = d.GetProcessNames(om.ReplicaSet{}, rs.Name)
			// error means that replica set is not in the deployment - it's ok, and we can proceed (could happen if
			// deletion cleanup happened twice and the first one cleaned OM state already)
			if e := d.RemoveReplicaSetByName(rs.Name, log); e != nil {
				log.Warnf("Failed to remove replica set from automation config: %s", e)
			}

			return nil
		},
		log,
	)
	if err != nil {
		return err
	}

	// Collect errors during cleanup but continue with all cleanup steps.
	// This ensures we attempt all cleanup operations even if some fail.
	var errs error

	if err := om.WaitForReadyState(conn, processNames, false, log); err != nil {
		errs = multierror.Append(errs, xerrors.Errorf("failed to wait for ready state. Continuing with cleanup: %w", err))
	}

	if rs.Spec.Backup != nil && rs.Spec.Backup.AutoTerminateOnDeletion {
		if err := backup.StopBackupIfEnabled(conn, conn, rs.Name, backup.ReplicaSetType, log); err != nil {
			errs = multierror.Append(errs, xerrors.Errorf("failed to stop backup. Continuing with cleanup: %w", err))
		}
	}

	// During deletion, calculate the maximum number of hosts that could possibly exist to ensure complete cleanup.
	// Reading from Status here is appropriate since this is outside the reconciliation loop.
	hostsToRemove, _ := dns.GetDNSNames(rs.Name, rs.ServiceName(), rs.Namespace, rs.Spec.GetClusterDomain(), util.MaxInt(rs.Status.Members, rs.Spec.Members), rs.Spec.GetExternalDomain())
	log.Infow("Stop monitoring removed hosts in Ops Manager", "removedHosts", hostsToRemove)

	if err := host.StopMonitoring(conn, hostsToRemove, log); err != nil {
		// StopMonitoring may fail with 401 if hosts are already removed or auth is misconfigured.
		errs = multierror.Append(errs, xerrors.Errorf("failed to stop monitoring for hosts %v. Continuing with cleanup: %w", hostsToRemove, err))
	}

	if err := r.reconciler.clearProjectAuthenticationSettings(ctx, conn, rs, processNames, log); err != nil {
		errs = multierror.Append(errs, xerrors.Errorf("failed to clear project authentication settings. Continuing with cleanup: %w", err))
	}

	log.Infow("Clear feature control for group: %s", "groupID", conn.GroupID())
	if result := controlledfeature.ClearFeatureControls(conn, conn.OpsManagerVersion(), log); !result.IsOK() {
		result.Log(log)
		log.Warnf("Failed to clear feature control from group: %s", conn.GroupID())
	}

	if errs != nil {
		log.Warnf("Replica set cleanup from Ops Manager completed with errors")
	} else {
		log.Info("Removed replica set from Ops Manager!")
	}
	return errs
}

func (r *ReconcileMongoDbReplicaSet) OnDelete(ctx context.Context, obj runtime.Object, log *zap.SugaredLogger) error {
	helper, err := r.newReconcilerHelper(ctx, obj.(*mdbv1.MongoDB), log)
	if err != nil {
		return err
	}
	return helper.OnDelete(ctx, obj, log)
}

func getAllHostsForReplicas(rs *mdbv1.MongoDB, membersCount int) []string {
	hostnames, _ := dns.GetDNSNames(rs.Name, rs.ServiceName(), rs.Namespace, rs.Spec.GetClusterDomain(), membersCount, rs.Spec.DbCommonSpec.GetExternalDomain())
	return hostnames
}

func (r *ReplicaSetReconcilerHelper) applySearchOverrides(ctx context.Context) (bool, error) {
	rs := r.resource
	log := r.log

	search, err := r.lookupCorrespondingSearchResource(ctx)
	if err != nil {
		return false, err
	}

	if search == nil {
		log.Debugf("No MongoDBSearch resource found, skipping search overrides")
		return false, nil
	}

	log.Infof("Applying search overrides from MongoDBSearch %s", search.NamespacedName())

	if rs.Spec.AdditionalMongodConfig == nil {
		rs.Spec.AdditionalMongodConfig = mdbv1.NewEmptyAdditionalMongodConfig()
	}
	searchMongodConfig := searchcontroller.GetMongodConfigParameters(search, rs.Spec.GetClusterDomain())
	rs.Spec.AdditionalMongodConfig.AddOption("setParameter", searchMongodConfig["setParameter"])

	return true, nil
}

func (r *ReplicaSetReconcilerHelper) mirrorKeyfileIntoSecretForMongot(ctx context.Context, d om.Deployment) error {
	rs := r.resource
	reconciler := r.reconciler
	log := r.log

	keyfileContents := maputil.ReadMapValueAsString(d, "auth", "key")
	keyfileSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%s", rs.Name, searchcontroller.MongotKeyfileFilename), Namespace: rs.Namespace}}

	log.Infof("Mirroring the replicaset %s's keyfile into the secret %s", rs.ObjectKey(), kube.ObjectKeyFromApiObject(keyfileSecret))

	_, err := controllerutil.CreateOrUpdate(ctx, reconciler.client, keyfileSecret, func() error {
		keyfileSecret.StringData = map[string]string{searchcontroller.MongotKeyfileFilename: keyfileContents}
		return controllerutil.SetOwnerReference(rs, keyfileSecret, reconciler.client.Scheme())
	})
	if err != nil {
		return xerrors.Errorf("failed to mirror the replicaset's keyfile into a secret: %w", err)
	}
	return nil
}

func (r *ReplicaSetReconcilerHelper) lookupCorrespondingSearchResource(ctx context.Context) (*searchv1.MongoDBSearch, error) {
	rs := r.resource
	reconciler := r.reconciler

	var search *searchv1.MongoDBSearch
	searchList := &searchv1.MongoDBSearchList{}
	if err := reconciler.client.List(ctx, searchList, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(searchv1.MongoDBSearchIndexFieldName, rs.GetNamespace()+"/"+rs.GetName()),
	}); err != nil {
		return nil, xerrors.Errorf("Failed to list MongoDBSearch resources referred in the MongoDB resource %s/%s. err : %v", rs.Namespace, rs.Name, err)
	}

	if len(searchList.Items) == 0 {
		return nil, nil
	}

	if len(searchList.Items) > 1 {
		return nil, xerrors.Errorf("Found multiple MongoDBSearch resources referred in sharded cluster %s/%s", rs.Namespace, rs.Name)
	}

	// this validates that there is exactly one MongoDBSearch pointing to this resource,
	// and that this resource passes search validations. If either fails, proceed without a search target
	// for the mongod automation config.
	if len(searchList.Items) == 1 {
		searchSource := searchcontroller.NewEnterpriseResourceSearchSource(rs)
		if searchSource.Validate() == nil {
			search = &searchList.Items[0]
		}
	}
	return search, nil
}

// monarchConfigHash returns a short SHA256 hash of the ConfigMap data to use
// as a pod template annotation, triggering a rolling restart when config changes.
func monarchConfigHash(data map[string]string) string {
	b, _ := json.Marshal(data)
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum[:8])
}
