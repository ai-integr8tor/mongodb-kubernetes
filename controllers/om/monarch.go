package om

import (
	"fmt"
	"strings"

	mdbv1 "github.com/mongodb/mongodb-kubernetes/api/v1/mdb"
)

// MaintainedMonarchComponents is the automation config section that tells the agent
// about the Monarch injector configuration for a standby cluster.
type MaintainedMonarchComponents struct {
	ReplicaSetID       string         `json:"replicaSetId"`
	ClusterPrefix      string         `json:"clusterPrefix"`
	InitialMode        string         `json:"initialMode"`
	AWSBucketName      string         `json:"awsBucketName"`
	AWSRegion          string         `json:"awsRegion"`
	AWSAccessKeyID     string         `json:"awsAccessKeyId"`
	AWSSecretAccessKey string         `json:"awsSecretAccessKey"`
	S3BucketEndpoint   string         `json:"s3BucketEndPoint,omitempty"`
	S3PathStyleAccess  bool           `json:"s3PathStyleAccess,omitempty"`
	InjectorConfig     InjectorConfig `json:"injectorConfig"`
}

type InjectorConfig struct {
	Version string          `json:"version"`
	SrcURI  string          `json:"srcURI,omitempty"`
	Shards  []InjectorShard `json:"shards"`
}

type InjectorShard struct {
	ShardID     string             `json:"shardId"`
	ReplSetName string             `json:"replSetName"`
	Instances   []InjectorInstance `json:"instances"`
}

type InjectorInstance struct {
	ID                 int    `json:"id"`
	Hostname           string `json:"hostname"`
	Disabled           bool   `json:"disabled"`
	Port               int    `json:"port"`
	ExternallyManaged  bool   `json:"externallyManaged"`
	HealthAPIEndpoint  string `json:"healthApiEndpoint"`
	MonarchAPIEndpoint string `json:"monarchApiEndpoint"`
}

// SetMaintainedMonarchComponents sets the maintainedMonarchComponents field in the automation config.
func (d Deployment) SetMaintainedMonarchComponents(mc []MaintainedMonarchComponents) {
	d["maintainedMonarchComponents"] = mc
}

// BuildMaintainedMonarchComponents builds the automation config entries for Monarch.
// For standby (injector) clusters, it creates one InjectorInstance per RS member.
// Each instance uses the member's FQDN as Hostname (for agent locality matching) but
// routes healthApiEndpoint and monarchApiEndpoint through the shared Service.
// For active (shipper) clusters, it creates entries without injector instances.
func BuildMaintainedMonarchComponents(mdb *mdbv1.MongoDB, rsName string, awsAccessKeyId string, awsSecretAccessKey string, memberHostnames []string, serviceDNS string) ([]MaintainedMonarchComponents, error) {
	monarch := mdb.Spec.Monarch
	if monarch == nil {
		return nil, fmt.Errorf("monarch spec is nil")
	}

	// InitialMode: "ACTIVE" for active clusters, "STANDBY" for standby clusters
	initialMode := "STANDBY"
	if monarch.Role == mdbv1.MonarchRoleActive {
		initialMode = "ACTIVE"
	}

	// ReplicaSetID is the local RS name. Active and standby clusters in a DR pair
	// are linked via the shared s3.prefix (ClusterPrefix), not via ReplicaSetID.
	mc := MaintainedMonarchComponents{
		ReplicaSetID:       rsName,
		ClusterPrefix:      monarch.S3.GetPrefix(rsName),
		InitialMode:        initialMode,
		AWSBucketName:      monarch.S3.Bucket,
		AWSRegion:          monarch.S3.Region,
		AWSAccessKeyID:     awsAccessKeyId,
		AWSSecretAccessKey: awsSecretAccessKey,
		S3BucketEndpoint:   monarch.S3.Endpoint,
		S3PathStyleAccess:  monarch.S3.PathStyle,
	}

	// Extract version from image tag (e.g., "quay.io/mongodb/monarch:0.1.1" -> "0.1.1")
	version := extractVersionFromImage(monarch.Image)

	if monarch.Role == mdbv1.MonarchRoleActive {
		// Active clusters use shipper.
		mc.InjectorConfig = InjectorConfig{
			Version: version,
			Shards:  []InjectorShard{},
		}
	} else {
		// Standby clusters need injector instance configuration.
		// In MCK, the injector runs as a separate Deployment behind a shared K8s Service.
		// Unlike the EA setup (where one injector runs on each mongod host), we have a
		// single Service endpoint that load-balances to injector pods.
		//
		// We create ONE injector instance pointing to the Service DNS. MongoDB RS will
		// have this single injector member added with voting rights. The K8s Service
		// provides high availability via its pod selector.
		instances := []InjectorInstance{
			{
				ID:                 0,
				Hostname:           serviceDNS, // Use Service DNS - injector is a separate Deployment
				Port:               9995,
				ExternallyManaged:  true,
				HealthAPIEndpoint:  serviceDNS + ":8080",
				MonarchAPIEndpoint: serviceDNS + ":1122",
			},
		}

		mc.InjectorConfig = InjectorConfig{
			Version: version,
			Shards: []InjectorShard{
				{
					ShardID:     "0",
					ReplSetName: rsName,
					Instances:   instances,
				},
			},
		}
	}

	return []MaintainedMonarchComponents{mc}, nil
}

// extractVersionFromImage extracts the tag from a container image reference.
// e.g., "quay.io/mongodb/monarch:0.1.1" -> "0.1.1"
// If no tag is found, returns "latest".
func extractVersionFromImage(image string) string {
	// Handle digest references (image@sha256:...)
	if idx := strings.LastIndex(image, "@"); idx != -1 {
		return "latest" // digest-based images don't have a version tag
	}
	// Handle tag references (image:tag)
	if idx := strings.LastIndex(image, ":"); idx != -1 {
		// Make sure we're not matching a port in the registry (e.g., localhost:5000/image)
		tag := image[idx+1:]
		if !strings.Contains(tag, "/") {
			return tag
		}
	}
	return "latest"
}
