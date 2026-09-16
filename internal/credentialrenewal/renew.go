package credentialrenewal

import (
	"context"
	"fmt"
	"path"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Target struct {
	Secret         string `json:"secret"`
	Namespace      string `json:"namespace"`
	ServiceAccount string `json:"serviceAccount"`
	MountPath      string `json:"mountPath"`
}

const ExpiresKey = "ocp.services.open-control-plane.io/credential-expires-at"

func Renew(ctx context.Context, host kubernetes.Interface, namespace string, target Target, connect func(*rest.Config) (kubernetes.Interface, error), now time.Time) error {
	if target.Secret == "" || target.Namespace == "" || target.ServiceAccount == "" || !path.IsAbs(target.MountPath) {
		return fmt.Errorf("invalid credential target")
	}
	secret, err := host.CoreV1().Secrets(namespace).Get(ctx, target.Secret, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if expires, err := time.Parse(time.RFC3339, secret.Annotations[ExpiresKey]); err == nil && expires.After(now.Add(12*time.Hour)) && len(secret.Data["mounted-kubeconfig"]) > 0 {
		return nil
	}
	cfg, err := clientcmd.Load(secret.Data["kubeconfig"])
	if err != nil {
		return fmt.Errorf("invalid kubeconfig in %s", target.Secret)
	}
	current := cfg.Contexts[cfg.CurrentContext]
	if current == nil || cfg.AuthInfos[current.AuthInfo] == nil {
		return fmt.Errorf("missing current credential")
	}
	auth := cfg.AuthInfos[current.AuthInfo]
	if auth.Token == "" || auth.Exec != nil || auth.AuthProvider != nil || auth.TokenFile != "" {
		return fmt.Errorf("expected an inline bootstrap token")
	}
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(secret.Data["kubeconfig"])
	if err != nil {
		return fmt.Errorf("invalid REST configuration")
	}
	if restConfig.Insecure {
		return fmt.Errorf("certificate verification is required")
	}
	restConfig.Timeout = 30 * time.Second
	remote, err := connect(restConfig)
	if err != nil {
		return fmt.Errorf("cannot create credential client")
	}
	duration := int64(86400)
	request, err := remote.CoreV1().ServiceAccounts(target.Namespace).CreateToken(ctx, target.ServiceAccount, &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: &duration}}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("token renewal failed for %s: %w", target.Secret, err)
	}
	if request.Status.Token == "" || !request.Status.ExpirationTimestamp.After(now.Add(time.Hour)) {
		return fmt.Errorf("replacement credential expires too soon")
	}
	auth.Token = request.Status.Token
	inline, err := clientcmd.Write(*cfg)
	if err != nil {
		return err
	}
	auth.Token = ""
	auth.TokenFile = path.Join(target.MountPath, "token")
	mounted, err := clientcmd.Write(*cfg)
	if err != nil {
		return err
	}
	secret.Data["kubeconfig"] = inline
	secret.Data["mounted-kubeconfig"] = mounted
	secret.Data["token"] = []byte(request.Status.Token)
	if secret.Annotations == nil {
		secret.Annotations = map[string]string{}
	}
	secret.Annotations[ExpiresKey] = request.Status.ExpirationTimestamp.UTC().Format(time.RFC3339)
	_, err = host.CoreV1().Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{})
	return err
}
