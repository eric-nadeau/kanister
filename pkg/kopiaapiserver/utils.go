package kopiaapiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jpillora/backoff"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"

	crv1alpha1 "github.com/kanisterio/kanister/pkg/apis/cr/v1alpha1"
	"github.com/kanisterio/kanister/pkg/consts"
	"github.com/kanisterio/kanister/pkg/field"
	"github.com/kanisterio/kanister/pkg/format"
	"github.com/kanisterio/kanister/pkg/function"
	"github.com/kanisterio/kanister/pkg/kopia"
	"github.com/kanisterio/kanister/pkg/kube"
	"github.com/kanisterio/kanister/pkg/log"
	"github.com/kanisterio/kanister/pkg/param"
	"github.com/kanisterio/kanister/pkg/poll"
	"github.com/kanisterio/kanister/pkg/utils"
)

const (
	dataMoverServicePort     = 51515
	dataMoverServiceProtocol = "TCP"
)

// getDataMoverServiceAddress returns the Kopia API server address to be used by the Kopia client
func getDataMoverServiceAddress(ctx context.Context, cli kubernetes.Interface, namespace, svcName string) (string, error) {
	svc, err := cli.CoreV1().Services(namespace).Get(ctx, svcName, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to get service %s from namespace %s", svcName, namespace))
	}
	return fmt.Sprintf(serverAddressFormat, svc.Spec.ClusterIP, dataMoverServicePort), nil
}

// getDataMoverPodAddress returns the local address to be used to start the Kopia API server
func getDataMoverPodAddress(ctx context.Context, cli kubernetes.Interface, namespace, podName string) (string, error) {
	p, err := cli.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to get pod %s from namespace %s", podName, namespace))
	}
	return fmt.Sprintf(serverAddressFormat, p.Status.PodIP, dataMoverServicePort), nil
}

// newDataMoverService returns a service to expose the Kopia API server pod to the application namespace
func newDataMoverService(appNamespace, namespace, serviceName string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
			Labels: map[string]string{
				appNamespaceLabelKey:     appNamespace,
				dataMoverServiceNameKey:  serviceName,
				dataMoverServiceLabelKey: dataMoverServiceLabelValue,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     dataMoverPrefix + "port",
					Protocol: dataMoverServiceProtocol,
					Port:     dataMoverServicePort,
				},
			},
			Selector: map[string]string{
				appNamespaceLabelKey:     appNamespace,
				dataMoverServiceNameKey:  serviceName,
				dataMoverServiceLabelKey: dataMoverServiceLabelValue,
			},
		},
	}
}

// newDataMoverNetworkPolicy returns a network policy with appropriate pod selector
func newDataMoverNetworkPolicy(appNamespace, namespace, svcName string) *networkingv1.NetworkPolicy {
	protocolTCP := corev1.ProtocolTCP
	port := intstr.FromInt(dataMoverServicePort)
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: dataMoverPrefix,
			Namespace:    namespace,
			Labels: map[string]string{
				appNamespaceLabelKey:     appNamespace,
				dataMoverServiceNameKey:  svcName,
				dataMoverServiceLabelKey: dataMoverServiceLabelValue,
			},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					appNamespaceLabelKey:     appNamespace,
					dataMoverServiceNameKey:  svcName,
					dataMoverServiceLabelKey: dataMoverServiceLabelValue,
				},
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{},
					Ports: []networkingv1.NetworkPolicyPort{
						{
							Protocol: &protocolTCP,
							Port:     &port,
						},
					},
				},
			},
		},
	}
}

func setupUserPassphraseSecet(
	ctx context.Context,
	cli kubernetes.Interface,
	apiServerInfo *APIServerInfo,
	hostnameList []string,
	namespace string,
) (map[string][]byte, error) {
	us, hostnameToPassphrase, err := createUserPassphraseSecret(ctx, cli, namespace, hostnameList)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kopia API client user passphrase secret")
	}
	apiServerInfo.UserPassphraseSecret = us

	return hostnameToPassphrase, nil
}

func setupServerPassphrase(
	ctx context.Context,
	cli kubernetes.Interface,
	apiServerInfo *APIServerInfo,
	namespace string,
	initialPassphrase []byte,
) error {
	serverPassphraseSecret, err := createServerPassphraseSecret(ctx, cli, namespace, initialPassphrase)
	if err != nil {
		return errors.Wrap(err, "Failed to create Kopia API client server passphrase secret")
	}
	apiServerInfo.ServerPassphraseSecret = serverPassphraseSecret
	return nil
}

func getLocationAndCredential(
	profile *param.Profile,
) (*crv1alpha1.Location, *param.Credential) {
	location := profile.Location
	credential := profile.Credential
	return &location, &credential
}

func createAPIServerPod(
	ctx context.Context,
	cli kubernetes.Interface,
	apiServerInfo *APIServerInfo,
	podOverride map[string]interface{},
	location *crv1alpha1.Location,
	credential *param.Credential,
	hostnameList []string,
	appNamespace,
	namespace string,
) (*corev1.Pod, map[string][]byte, error) {
	// Step 1: Expose the Kopia API server pod to the application namespace
	// 1.a: Create a Service
	serviceName := fmt.Sprint(dataMoverPrefix, rand.String(5))
	svc, err := createDataMoverService(ctx, cli, appNamespace, namespace, serviceName)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to create Kopia API server service")
	}
	apiServerInfo.ServiceName = svc.Name
	// 1.b: Get Service address with exposed port
	svcAddress, err := getDataMoverServiceAddress(ctx, cli, namespace, svc.Name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to get Kopia API server service address")
	}
	apiServerInfo.ServiceAddress = svcAddress

	// Step 2: Add a NetworkPolicy to allow traffic into the pod from application namespace
	np, err := createDataMoverNetworkPolicy(ctx, cli, appNamespace, namespace, svc.Name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to create Kopia API server network policy")
	}
	apiServerInfo.NetworkPolicyName = np.Name

	// Step 3: Pass reference of podOverride to directly unmarshal into it.
	err = addCertConfigurationInPodOverride(&podOverride)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to attach Certificate Configuration")
	}

	// Step 4: Create the Kopia API server pod
	options := &kube.PodOptions{
		Namespace:    namespace,
		GenerateName: dataMoverPrefix,
		Image:        function.KanisterToolsImage(),
		Command:      []string{"bash", "-c", "tail -f /dev/null"},
		PodOverride:  podOverride,
		Labels: map[string]string{
			appNamespaceLabelKey:     appNamespace,
			dataMoverServiceNameKey:  svc.Name,
			dataMoverServiceLabelKey: dataMoverServiceLabelValue,
		},
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pod, err := kube.CreatePod(ctx, cli, options)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to create Kopia API server pod")
	}
	apiServerInfo.PodName = pod.Name

	// Step 5: Generate a secret with passphrases for all Kopia clients
	hostnameToPassphrase, err := setupUserPassphraseSecet(ctx, cli, apiServerInfo, hostnameList, namespace)
	if err != nil {
		return nil, nil, err
	}

	// Step 6: Pass the reference to the secret with TLS Certificates details
	certsSecret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tlsCertSecretName,
			Namespace: namespace,
		},
	}
	apiServerInfo.CertsSecret = &certsSecret

	// Step 7: Wait for the created pod to reach the ready state
	if err := kube.WaitForPodReady(ctx, cli, pod.Namespace, pod.Name); err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("Failed while waiting for Pod %s to be ready", pod.Name))
	}

	container := pod.Spec.Containers[0].Name
	// Step 8: Copy Google creds to the pod
	if location.Type == crv1alpha1.LocationTypeGCS {
		gcsServiceKey := credential.KeyPair.Secret
		pw := kube.NewPodWriter(cli, consts.GoogleCloudCredsFilePath, bytes.NewBufferString(gcsServiceKey))
		if err := pw.Write(ctx, pod.Namespace, pod.Name, container); err != nil {
			return nil, nil, errors.Wrap(err, "Failed to write Google credential file into the pod")
		}
	}

	return pod, hostnameToPassphrase, nil
}

func initializeAPIServer(
	ctx context.Context,
	cli kubernetes.Interface,
	pod *corev1.Pod,
	hostnameToPassphrase map[string][]byte,
	namespace,
	repoPassword string,
) ([]byte, error) {
	container := pod.Spec.Containers[0].Name
	// Get the PodIP to be used as Kopia API server address
	serverAddress, err := getDataMoverPodAddress(ctx, cli, pod.Namespace, pod.Name)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get Kopia API server pod address")
	}

	initialUsername := GetDefaultServerUsername()

	initialPassphrase, err := utils.NewRandomAlphaNumericKey(utils.MinimumAlphaNumericKeyLength)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kopia API server initial passphrase")
	}

	// Start the Kopia API server
	cmd := kopia.ServerStartCommand(
		kopia.DefaultConfigFilePath,
		kopia.DefaultLogDirectory,
		serverAddress,
		tlsCertPath,
		tlsKeyPath,
		initialUsername,
		string(initialPassphrase),
		false,
		true,
	)
	stdout, stderr, err := kube.Exec(cli, namespace, pod.Name, container, cmd, nil)
	format.Log(pod.Name, container, stdout)
	format.Log(pod.Name, container, stderr)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to start Kopia API server")
	}

	fingerprint, err := kopia.ExtractFingerprintFromCertSecret(ctx, cli, tlsCertSecretName, namespace)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to extract fingerprint Kopia API Server Certificate Secret Data")
	}

	// Wait for server to start
	cmd = kopia.ServerStatusCommand(
		kopia.DefaultConfigFilePath,
		kopia.DefaultLogDirectory,
		serverAddress,
		initialUsername,
		string(initialPassphrase),
		fingerprint,
	)

	ctx, cancel := context.WithTimeout(ctx, DefaultServerStartTimeout)
	defer cancel()

	err = WaitTillCommandSucceed(ctx, cli, cmd, namespace, pod.Name, container)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to wait for the Kopia API server to start")
	}

	// The users added to API server are persisted in repository. Adding the same user again
	// fails. Since we generate new random password for every new run, it does not match with
	// the existing one and gives unauthorized error. So we delete users that exist with same
	// username and add again with updated password.

	// Run list server user command
	cmd = kopia.ServerListUserCommand(
		repoPassword,
		kopia.DefaultConfigFilePath,
		kopia.DefaultLogDirectory,
	)
	stdout, stderr, err = kube.Exec(cli, namespace, pod.Name, container, cmd, nil)
	format.Log(pod.Name, container, stdout)
	format.Log(pod.Name, container, stderr)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to list users from the Kopia repository")
	}

	userProfiles := []kopia.KopiaUserProfile{}

	err = json.Unmarshal([]byte(stdout), &userProfiles)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal user list")
	}

	// Get list of usernames from ServerListUserCommand output to update the existing data with updated password
	existingUserHostList := sets.String{}
	for _, userProfile := range userProfiles {
		existingUserHostList.Insert(userProfile.Username)
	}

	// Add all users to server
	for hostname, passphrase := range hostnameToPassphrase {
		serverUsername := fmt.Sprintf(ServerUsernameFormat, K10AdminUsername, hostname)
		// Check if user already exists
		if existingUserHostList.Has(serverUsername) {
			log.Debug().Print("User already exists, updating passphrase", field.M{"serverUsername": serverUsername})
			// Update password for the existing user
			cmd := kopia.ServerSetUserCommand(
				repoPassword,
				kopia.DefaultConfigFilePath,
				kopia.DefaultLogDirectory,
				serverUsername,
				string(passphrase),
			)
			stdout, stderr, err := kube.Exec(cli, namespace, pod.Name, container, cmd, nil)
			format.Log(pod.Name, container, stdout)
			format.Log(pod.Name, container, stderr)
			if err != nil {
				return nil, errors.Wrap(err, "Failed to update existing user passphrase from the Kopia API server")
			}
			continue
		}
		cmd := kopia.ServerAddUserCommand(
			repoPassword,
			kopia.DefaultConfigFilePath,
			kopia.DefaultLogDirectory,
			serverUsername,
			string(passphrase),
		)
		stdout, stderr, err := kube.Exec(cli, namespace, pod.Name, container, cmd, nil)
		format.Log(pod.Name, container, stdout)
		format.Log(pod.Name, container, stderr)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to add new user to the Kopia API server")
		}
	}

	// Run server refresh
	cmd = kopia.ServerRefreshCommand(
		repoPassword,
		kopia.DefaultConfigFilePath,
		kopia.DefaultLogDirectory,
		serverAddress,
		initialUsername,
		string(initialPassphrase),
		fingerprint,
	)
	stdout, stderr, err = kube.Exec(cli, namespace, pod.Name, container, cmd, nil)
	format.Log(pod.Name, container, stdout)
	format.Log(pod.Name, container, stderr)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to refresh Kopia API server")
	}
	return initialPassphrase, nil
}

func addCertConfigurationInPodOverride(podOverride *map[string]interface{}) error {
	podSpecBytes, err := json.Marshal(*podOverride)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal Pod Override")
	}

	var podOverrideSpec corev1.PodSpec
	if err := json.Unmarshal(podSpecBytes, &podOverrideSpec); err != nil {
		return errors.Wrap(err, "Failed to unmarshal Pod Override Spec")
	}

	podOverrideSpec.Volumes = append(podOverrideSpec.Volumes, corev1.Volume{
		Name: tlsCertVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: tlsCertSecretName,
			},
		},
	})

	podOverrideSpec.Volumes = append(podOverrideSpec.Volumes, corev1.Volume{
		Name: tlsKeyVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: tlsKeySecretName,
			},
		},
	})

	if len(podOverrideSpec.Containers) == 0 {
		podOverrideSpec.Containers = append(podOverrideSpec.Containers, corev1.Container{
			Name: "container",
		})
	}

	podOverrideSpec.Containers[0].VolumeMounts = append(podOverrideSpec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      tlsCertVolumeName,
		MountPath: tlsCertDefaultMountPath,
	})

	podOverrideSpec.Containers[0].VolumeMounts = append(podOverrideSpec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      tlsKeyVolumeName,
		MountPath: tlsKeyDefaultMountPath,
	})

	podSpecBytes, err = json.Marshal(podOverrideSpec)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal Pod Override Spec")
	}

	if err := json.Unmarshal(podSpecBytes, podOverride); err != nil {
		return errors.Wrap(err, "Failed to unmarshal Pod Override")
	}

	return nil
}

// WaitForServerPodShutdown waits for the API server pod for the given application to shutdown
func WaitForServerPodShutdown(ctx context.Context, cli kubernetes.Interface, namespace, applicationName string) error {
	ctx, cancel := context.WithTimeout(ctx, maxWaitDuration)
	defer cancel()
	label := metav1.FormatLabelSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{
			appNamespaceLabelKey: applicationName,
		},
	})
	log.Debug().Print("Waiting for running Kopia API server pod to shutdown", field.M{"applicationName": applicationName})
	err := poll.Wait(ctx, func(ctx context.Context) (bool, error) {
		pods, err := cli.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: label})
		if err != nil {
			return false, errors.Wrap(err, "Failed to list Kopia API server pods")
		}
		if len(pods.Items) == 0 {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return errors.Wrap(err, "Failed while waiting for Kopia API server pod to shutdown")
	}
	return nil
}

// GetDefaultServerUsername returns the default server username used to run Kopia API Server commands
func GetDefaultServerUsername() string {
	return fmt.Sprintf(ServerUsernameFormat, K10AdminUsername, defaultServerHostname)
}

// createUserPassphraseSecret creates a secret in the K10 namespace with generated passphrases for all Kopia clients connecting to the server
func createUserPassphraseSecret(ctx context.Context, cli kubernetes.Interface, namespace string, hostnameList []string) (*corev1.Secret, map[string][]byte, error) {
	hostnameToPassphrase := make(map[string][]byte, len(hostnameList))
	for _, hostname := range hostnameList {
		userPassphrase, err := utils.NewRandomAlphaNumericKey(utils.MinimumAlphaNumericKeyLength)
		if err != nil {
			return nil, nil, errors.Wrap(err, "Failed to generate Kopia API client user passphrase")
		}
		hostnameToPassphrase[hostname] = userPassphrase
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: userPassphraseSecretNamePrefix,
			Namespace:    namespace,
		},
		Type: "Opaque",
		Data: hostnameToPassphrase,
	}
	sec, err := cli.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to create Kopia API client user passphrase secret")
	}
	return sec, hostnameToPassphrase, nil
}

// createServerPassphraseSecret creates a secret in the K10 namespace with generated passphrase for initial Kopia server user
func createServerPassphraseSecret(ctx context.Context, cli kubernetes.Interface, namespace string, passphrase []byte) (*corev1.Secret, error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: serverPassphraseSecretNamePrefix,
			Namespace:    namespace,
		},
		Type: "Opaque",
		Data: map[string][]byte{
			KopiaServerPassphraseSecretKey: passphrase,
		},
	}
	return cli.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
}

// createDataMoverService creates a service with appropriate labels
func createDataMoverService(ctx context.Context, cli kubernetes.Interface, appNamespace, namespace, serviceName string) (*corev1.Service, error) {
	svc, err := cli.CoreV1().Services(namespace).Create(ctx, newDataMoverService(appNamespace, namespace, serviceName), metav1.CreateOptions{})
	if err != nil {
		return svc, err
	}
	err = poll.WaitWithBackoff(ctx, backoff.Backoff{
		Factor: 2,
		Jitter: false,
		Min:    100 * time.Millisecond,
		Max:    15 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		_, err := cli.CoreV1().Endpoints(namespace).Get(ctx, svc.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			return false, nil
		case err != nil:
			return false, err
		}
		return true, nil
	})

	return svc, err
}

// createDataMoverNetworkPolicy creates a network policy to allow application namespace traffic into the Kopia API server pod
func createDataMoverNetworkPolicy(ctx context.Context, cli kubernetes.Interface, appNamespace, namespace, svcName string) (*networkingv1.NetworkPolicy, error) {
	np := newDataMoverNetworkPolicy(appNamespace, namespace, svcName)
	return cli.NetworkingV1().NetworkPolicies(namespace).Create(ctx, np, metav1.CreateOptions{})
}

// WaitTillCommandSucceed returns error if the Command fails to pass without error before default timeout
func WaitTillCommandSucceed(ctx context.Context, cli kubernetes.Interface, cmd []string, namespace, podName, container string) error {
	err := poll.WaitWithBackoff(ctx, backoff.Backoff{
		Factor: 2,
		Jitter: false,
		Min:    100 * time.Millisecond,
		Max:    180 * time.Second,
	}, func(context.Context) (bool, error) {
		stdout, stderr, exErr := kube.Exec(cli, namespace, podName, container, cmd, nil)
		format.Log(podName, container, stdout)
		format.Log(podName, container, stderr)
		if exErr != nil {
			return false, nil
		}
		return true, nil
	})
	return errors.Wrap(err, "Failed while waiting for Kopia API server to start")
}
