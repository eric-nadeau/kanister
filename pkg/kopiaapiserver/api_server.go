package kopiaapiserver

import (
	"context"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	crv1alpha1 "github.com/kanisterio/kanister/pkg/apis/cr/v1alpha1"
	"github.com/kanisterio/kanister/pkg/kopia"
	"github.com/kanisterio/kanister/pkg/param"
)

const (
	// K10AdminUsername is the username for the user with Admin privileges
	K10AdminUsername = "k10-admin"
	// ServerUsernameFormat is used to construct server username for Kopia API Server Status Command
	ServerUsernameFormat = "%s@%s"
	// DefaultServerStartTimeout is default time to create context for Kopia API Server Status Command
	DefaultServerStartTimeout = 120 * time.Second

	appNamespaceLabelKey             = "app-name"
	dataMoverPrefix                  = "data-mover-svc-"
	dataMoverServiceLabelKey         = "service"
	dataMoverServiceLabelValue       = "data-mover-svc"
	dataMoverServiceNameKey          = "name"
	defaultServerHostname            = "data-mover-server-pod"
	maxWaitDuration                  = 15 * time.Minute
	serverAddressFormat              = "https://%s:%d"
	serverPassphraseSecretNamePrefix = "data-mover-server-passphrase-"
	userPassphraseSecretNamePrefix   = "data-mover-user-passphrase-"
	tlsCertSecretName                = "kopia-tls-cert"
	tlsKeySecretName                 = "kopia-tls-key"
	tlsCertVolumeName                = "kopia-cert"
	tlsKeyVolumeName                 = "kopia-key"
	tlsCertDefaultMountPath          = "/etc/certs/cert"
	tlsKeyDefaultMountPath           = "/etc/certs/key"
	tlsKeyPath                       = "/etc/certs/key/tls.key"
	tlsCertPath                      = "/etc/certs/cert/tls.crt"

	// Kopia client/server APIServerInfo fields
	KopiaAPIServerAddressArg         = "serverAddress"
	KopiaUserPassphraseArg           = "userPassphrase"
	KopiaUserPassphraseSecretDataKey = "key"
	KopiaUserPassphraseSecretKey     = "userPassphraseKey"
	KopiaTLSCertSecretKey            = "certs"
	KopiaTLSCertSecretDataArg        = "certData"
	KopiaServerPassphraseArg         = "serverPassphrase"
	KopiaServerPassphraseSecretKey   = "serverPassphraseKey"
)

type APIServerInfo struct {
	PodName                string
	NetworkPolicyName      string
	ServiceAddress         string
	ServiceName            string
	ServerPassphraseSecret *corev1.Secret
	UserPassphraseSecret   *corev1.Secret
	CertsSecret            *corev1.Secret
}

func SetupAPIServer(
	ctx context.Context,
	cli kubernetes.Interface,
	profile *param.Profile,
	appNamespace,
	backupArtifactPrefix,
	namespace,
	repoPassword string,
	hostnameList []string,
	podOverride map[string]interface{},
) (*APIServerInfo, error) {
	apiServerInfo := &APIServerInfo{}
	var (
		err error
	)
	defer func() {
		if err != nil {
			CleanupAPIServer(context.TODO(), cli, apiServerInfo, namespace)
		}
	}()

	location, credential := getLocationAndCredential(profile)

	pod, hostnameToPassphrase, err := createAPIServerPod(ctx, cli, apiServerInfo, podOverride, location, credential, hostnameList, appNamespace, namespace)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kopia API Server Pod")
	}

	// Initialize the repository and start the Kopia API server
	initialPassphrase, err := setupAPIServer(
		ctx,
		cli,
		location,
		credential,
		pod,
		backupArtifactPrefix,
		namespace,
		repoPassword,
		hostnameToPassphrase,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize Kopia API server")
	}

	err = setupServerPassphrase(ctx, cli, apiServerInfo, namespace, initialPassphrase)
	if err != nil {
		return nil, err
	}
	return apiServerInfo, nil
}

func setupAPIServer(
	ctx context.Context,
	cli kubernetes.Interface,
	location *crv1alpha1.Location,
	credential *param.Credential,
	pod *corev1.Pod,
	backupArtifactPrefix,
	namespace,
	repoPassword string,
	hostnameToPassphrase map[string][]byte,
) ([]byte, error) {
	contentCacheMB, metadataCacheMB := kopia.GetCacheSizeSettingsForSnapshot()

	if err := kopia.ConnectToOrCreateKopiaRepository(
		cli,
		namespace,
		pod.Name,
		pod.Spec.Containers[0].Name,
		backupArtifactPrefix,
		repoPassword,
		defaultServerHostname,
		K10AdminUsername,
		kopia.DefaultCacheDirectory,
		kopia.DefaultConfigFilePath,
		kopia.DefaultLogDirectory,
		contentCacheMB,
		metadataCacheMB,
		&kopia.KanisterProfile{Profile: &param.Profile{Location: *location, Credential: *credential}},
	); err != nil {
		return nil, err
	}
	return initializeAPIServer(ctx, cli, pod, hostnameToPassphrase, namespace, repoPassword)
}

// CleanupAPIServer cleans up all the resources created for the Kopia API server
func CleanupAPIServer(ctx context.Context, cli kubernetes.Interface, info *APIServerInfo, namespace string) {
	if info == nil {
		// No action required
		return
	}
	if info.PodName != "" {
		cli.CoreV1().Pods(namespace).Delete(ctx, info.PodName, metav1.DeleteOptions{}) // nolint: errcheck
	}
	if info.NetworkPolicyName != "" {
		cli.NetworkingV1().NetworkPolicies(namespace).Delete(ctx, info.NetworkPolicyName, metav1.DeleteOptions{}) // nolint: errcheck
	}
	if info.ServiceName != "" {
		cli.CoreV1().Services(namespace).Delete(ctx, info.ServiceName, metav1.DeleteOptions{}) // nolint: errcheck
	}
	if info.UserPassphraseSecret != nil {
		cli.CoreV1().Secrets(namespace).Delete(ctx, info.UserPassphraseSecret.Name, metav1.DeleteOptions{}) // nolint: errcheck
	}
	if info.ServerPassphraseSecret != nil {
		cli.CoreV1().Secrets(namespace).Delete(ctx, info.ServerPassphraseSecret.Name, metav1.DeleteOptions{}) // nolint: errcheck
	}
}

// ValidateAPIServer returns error if any component of the Kopia API server is not setup correctly
func ValidateAPIServer(ctx context.Context, cli kubernetes.Interface, info *APIServerInfo, namespace string) error {
	if info == nil {
		return errors.New("Kopia API ServerInfo must not be nil")
	}
	if info.PodName == "" {
		return errors.New("Kopia API ServerInfo Pod name must not be empty")
	}
	if _, err := cli.CoreV1().Pods(namespace).Get(ctx, info.PodName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		return errors.New("Kopia API ServerInfo Pod does not exist")
	}
	if info.NetworkPolicyName == "" {
		return errors.New("Kopia API ServerInfo NetworkPolicy name must not be empty")
	}
	if _, err := cli.NetworkingV1().NetworkPolicies(namespace).Get(ctx, info.NetworkPolicyName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		return errors.New("Kopia API ServerInfo NetworkPolicy does not exist")
	}
	if info.ServiceAddress == "" {
		return errors.New("Kopia API ServerInfo Service address must not be empty")
	}
	if info.ServiceName == "" {
		return errors.New("Kopia API ServerInfo Service name must not be empty")
	}
	if _, err := cli.CoreV1().Services(namespace).Get(ctx, info.ServiceName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		return errors.New("Kopia API ServerInfo Service does not exist")
	}
	if info.ServerPassphraseSecret == nil {
		return errors.New("Kopia API ServerInfo ServerPassphrase secret must not be nil")
	}
	if _, err := cli.CoreV1().Secrets(namespace).Get(ctx, info.ServerPassphraseSecret.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		return errors.New("Kopia API ServerInfo ServerPassphrase secret does not exist")
	}
	if info.UserPassphraseSecret == nil {
		return errors.New("Kopia API ServerInfo UserPassphrase secret must not be nil")
	}
	if _, err := cli.CoreV1().Secrets(namespace).Get(ctx, info.UserPassphraseSecret.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		return errors.New("Kopia API ServerInfo UserPassphrase secret does not exist")
	}
	return nil
}
