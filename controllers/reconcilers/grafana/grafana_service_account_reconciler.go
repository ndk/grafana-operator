package grafana

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	genapi "github.com/grafana/grafana-openapi-client-go/client"
	"github.com/grafana/grafana-openapi-client-go/client/service_accounts"
	"github.com/grafana/grafana-openapi-client-go/models"
	v1beta1 "github.com/grafana/grafana-operator/v5/api/v1beta1"
	client2 "github.com/grafana/grafana-operator/v5/controllers/client"
)

// conditionServiceAccountsSynced is set after service accounts and tokens are synced.
const conditionServiceAccountsSynced = "ServiceAccountsSynced"

// GrafanaServiceAccountReconciler syncs Grafana service accounts and their tokens.
type GrafanaServiceAccountReconciler struct {
	client client.Client
}

func NewGrafanaServiceAccountReconciler(c client.Client) *GrafanaServiceAccountReconciler {
	return &GrafanaServiceAccountReconciler{client: c}
}

func (r *GrafanaServiceAccountReconciler) Reconcile(
	ctx context.Context,
	cr *v1beta1.Grafana,
	_ *v1beta1.OperatorReconcileVars,
	_ *runtime.Scheme,
) (v1beta1.OperatorStageStatus, error) {
	ctx = logf.IntoContext(ctx, logf.FromContext(ctx).WithName("serviceAccountReconciler"))

	spec := cr.Spec.GrafanaServiceAccounts
	if spec == nil || len(spec.Accounts) == 0 {
		logf.FromContext(ctx).V(1).Info("No service accounts declared, skipping")
		return v1beta1.OperatorStageResultSuccess, nil
	}

	gClient, err := client2.NewGeneratedGrafanaClient(ctx, r.client, cr)
	if err != nil {
		return v1beta1.OperatorStageResultFailed, fmt.Errorf("building grafana client: %w", err)
	}

	for _, account := range spec.Accounts {
		ctx := logf.IntoContext(ctx, logf.FromContext(ctx).WithValues("account", account.Name))
		if err := r.ensureAccount(ctx, cr, gClient, spec.GenerateTokenSecret, account); err != nil {
			return v1beta1.OperatorStageResultFailed, err
		}
	}

	return v1beta1.OperatorStageResultSuccess, nil
}

// ensureAccount idempotently ensures that the service account exists in Grafana and all requested tokens are present.
// If GenerateTokenSecret is true, token values are stored in Secrets.
func (r *GrafanaServiceAccountReconciler) ensureAccount(
	ctx context.Context,
	grafana *v1beta1.Grafana,
	gClient *genapi.GrafanaHTTPAPI,
	genSecret bool,
	account v1beta1.GrafanaServiceAccount,
) error {
	logf.FromContext(ctx).V(1).Info("Ensuring service account exists")

	serviceAccountID, err := r.getOrCreateServiceAccount(ctx, gClient, account)
	if err != nil {
		return err
	}

	specTokens := account.Tokens
	if len(specTokens) == 0 && genSecret {
		logf.FromContext(ctx).V(1).Info("No tokens declared, creating default token")
		specTokens = []v1beta1.GrafanaServiceAccountToken{
			{Name: fmt.Sprintf("%s-token", account.Name)},
		}
	}

	for _, token := range specTokens {
		logf.FromContext(ctx).V(1).Info("Ensuring token exists", "token", token.Name)
		if err := r.getOrCreateToken(ctx, grafana, gClient, serviceAccountID, account, token, genSecret); err != nil {
			return err
		}
	}

	logf.FromContext(ctx).V(1).Info("All tokens ensured for service account")

	return nil
}

func (r *GrafanaServiceAccountReconciler) getOrCreateServiceAccount(
	ctx context.Context,
	gClient *genapi.GrafanaHTTPAPI,
	specAccount v1beta1.GrafanaServiceAccount,
) (int64, error) {
	search, err := gClient.ServiceAccounts.SearchOrgServiceAccountsWithPaging(
		service_accounts.NewSearchOrgServiceAccountsWithPagingParamsWithContext(ctx).
			WithQuery(&specAccount.Name),
	)
	if err != nil {
		return 0, fmt.Errorf("listing service accounts: %w", err)
	}

	for _, account := range search.Payload.ServiceAccounts {
		if account.Name == specAccount.Name {
			if account.Role != specAccount.Role {
				_, err := gClient.ServiceAccounts.UpdateServiceAccount(
					service_accounts.NewUpdateServiceAccountParamsWithContext(ctx).
						WithServiceAccountID(account.ID).
						WithBody(&models.UpdateServiceAccountForm{
							Role: specAccount.Role,
						}),
				)
				if err != nil {
					return 0, fmt.Errorf("updating service account role: %w", err)
				}
			}
			return account.ID, nil
		}
	}

	resp, err := gClient.ServiceAccounts.CreateServiceAccount(
		service_accounts.NewCreateServiceAccountParamsWithContext(ctx).
			WithBody(&models.CreateServiceAccountForm{
				Name: specAccount.Name,
				Role: specAccount.Role,
			}),
	)
	if err != nil {
		return 0, fmt.Errorf("creating service account: %w", err)
	}

	return resp.Payload.ID, nil
}

func (r *GrafanaServiceAccountReconciler) getOrCreateToken(
	ctx context.Context,
	grafana *v1beta1.Grafana,
	gClient *genapi.GrafanaHTTPAPI,
	serviceAccountID int64,
	specAccount v1beta1.GrafanaServiceAccount,
	token v1beta1.GrafanaServiceAccountToken,
	genSecret bool,
) error {
	secretName := token.Name
	if secretName == "" {
		secretName = fmt.Sprintf("%s-token", specAccount.Name)
	}

	existing := &corev1.Secret{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: grafana.Namespace, Name: secretName}, existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	cmd := models.AddServiceAccountTokenCommand{Name: secretName}
	if token.Expires != nil {
		sec := int64(time.Until(token.Expires.Time).Seconds())
		if sec > 0 {
			cmd.SecondsToLive = sec
		}
	}

	tokenResp, err := gClient.ServiceAccounts.CreateToken(
		service_accounts.NewCreateTokenParamsWithContext(ctx).
			WithServiceAccountID(serviceAccountID).
			WithBody(&cmd),
	)
	if err != nil {
		return fmt.Errorf("creating token %s: %w", secretName, err)
	}

	if !genSecret {
		return nil
	}

	newSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: grafana.Namespace,
			Labels:    map[string]string{"app.kubernetes.io/managed-by": "grafana-operator"},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"token": []byte(tokenResp.Payload.Key)},
	}
	if err := controllerutil.SetControllerReference(grafana, newSec, r.client.Scheme()); err != nil {
		return err
	}

	return r.client.Create(ctx, newSec)
}
