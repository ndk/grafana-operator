package controllers

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	kuberr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/grafana/grafana-operator/v5/api/v1beta1"
	"github.com/grafana/grafana-operator/v5/controllers/reconcilers/grafana"
)

const conditionServiceAccountsSynced = "ServiceAccountsSynchronized"

type GrafanaServiceAccountController struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *GrafanaServiceAccountController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithName("GrafanaServiceAccountController")
	ctx = logf.IntoContext(ctx, log)

	cr := &v1beta1.Grafana{}
	err := r.Get(ctx, req.NamespacedName, cr)
	if err != nil {
		if kuberr.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting Grafana: %w", err)
	}

	defer func() {
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &v1beta1.Grafana{}
			err := r.Client.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, latest)
			if err != nil {
				return fmt.Errorf("getting Grafana %s/%s: %w", cr.Namespace, cr.Name, err)
			}
			latest.Status.GrafanaServiceAccounts = cr.Status.GrafanaServiceAccounts
			return r.Status().Update(ctx, latest)
		})
		if err != nil {
			log.Error(err, "updating status")
		}
	}()

	meta.RemoveStatusCondition(&cr.Status.Conditions, conditionServiceAccountsSynced)
	if cr.Spec.GrafanaServiceAccounts == nil {
		cr.Status.GrafanaServiceAccounts = nil
		return ctrl.Result{}, nil
	}

	recon := grafana.NewGrafanaServiceAccountReconciler(r.Client)
	stage, err := recon.Reconcile(ctx, cr, nil, r.Scheme)

	cond := metav1.Condition{
		Type:               conditionServiceAccountsSynced,
		ObservedGeneration: cr.Generation,
		LastTransitionTime: metav1.Time{Time: time.Now()},
	}
	if err != nil || stage != v1beta1.OperatorStageResultSuccess {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "ApplyFailed"
		if err != nil {
			cond.Message = err.Error()
		} else {
			cond.Message = "reconcile failed"
		}
		meta.SetStatusCondition(&cr.Status.Conditions, cond)
		return ctrl.Result{}, fmt.Errorf("reconciling service accounts: %w", err)
	}

	cond.Status = metav1.ConditionTrue
	cond.Reason = "ApplySuccessful"
	cond.Message = "service accounts reconciled"
	meta.SetStatusCondition(&cr.Status.Conditions, cond)

	return ctrl.Result{}, nil
}

func (r *GrafanaServiceAccountController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.Grafana{},
			builder.WithPredicates(
				ignoreStatusUpdates(),
				serviceAccountSpecChanged(),
			)).
		Owns(&corev1.Secret{}).
		WithOptions(controller.Options{RateLimiter: defaultRateLimiter()}).
		Named("grafanaserviceaccount").
		Complete(r)
}
