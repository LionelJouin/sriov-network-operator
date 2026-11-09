package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	sriovdrav1alpha1 "github.com/k8snetworkplumbingwg/dra-driver-sriov/pkg/api/sriovdra/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	sriovv1 "github.com/k8snetworkplumbingwg/sriov-network-operator/api/v1"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/consts"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/dra"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/test/util/cluster"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/test/util/discovery"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/test/util/namespaces"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/test/util/network"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/test/util/pod"
)

const (
	draResourceName      = "draresource"
	initialVFs           = 2
	updatedVFs           = 4
	devicePluginDSName   = "sriov-device-plugin"
	draDriverAppLabel    = "app=sriov-dra-driver"
	devicePluginAppLabel = "app=sriov-device-plugin"
	// draClaimRefName is the pod-level claim reference name used by test pods.
	draClaimRefName = "claim-ref"
)

// isDeviceClassAPIAvailable reports whether resource.k8s.io/v1 is discoverable.
// It returns (false, nil) only when the API is conclusively unavailable (NotFound /
// NoMatch). Other discovery failures (auth, outage, etc.) are returned as errors.
func isDeviceClassAPIAvailable() (bool, error) {
	_, err := clients.ServerResourcesForGroupVersion(schema.GroupVersion{
		Group:   "resource.k8s.io",
		Version: "v1",
	}.String())
	if err == nil {
		return true, nil
	}
	if errors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return false, nil
	}
	return false, fmt.Errorf("discovering resource.k8s.io/v1 DeviceClass API: %w", err)
}

// draDriverInitContainersTerminated reports whether every init container on the pod
// has terminated successfully (exit code 0). Used to assert wait-for-config has completed.
// Returns false when the pod has no init containers, since wait-for-config is required.
func draDriverInitContainersTerminated(pod *corev1.Pod) bool {
	if len(pod.Spec.InitContainers) == 0 {
		return false
	}
	if len(pod.Status.InitContainerStatuses) < len(pod.Spec.InitContainers) {
		return false
	}
	for _, st := range pod.Status.InitContainerStatuses {
		if st.State.Terminated == nil || st.State.Terminated.ExitCode != 0 {
			return false
		}
	}
	return true
}

func listDRADriverPods(ctx context.Context) []corev1.Pod {
	pods, err := clients.Pods(operatorNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: draDriverAppLabel,
	})
	Expect(err).ToNot(HaveOccurred())
	return pods.Items
}

func countAdvertisedDRADevices(ctx context.Context, nodeName, resourceAttribute string) (int, error) {
	list := &resourceapi.ResourceSliceList{}
	if err := clients.List(ctx, list); err != nil {
		return 0, err
	}

	// During a pool update the driver may publish the old and new generations together.
	// Count only the highest generation for each pool on this node.
	generation := map[string]int64{}
	for i := range list.Items {
		slice := &list.Items[i]
		if !draSliceForNode(slice, nodeName) {
			continue
		}
		if slice.Spec.Pool.Generation > generation[slice.Spec.Pool.Name] {
			generation[slice.Spec.Pool.Name] = slice.Spec.Pool.Generation
		}
	}

	count := 0
	attrKey := resourceapi.QualifiedName(dra.ResourceNameAttributeKey)
	for i := range list.Items {
		slice := &list.Items[i]
		if !draSliceForNode(slice, nodeName) || slice.Spec.Pool.Generation != generation[slice.Spec.Pool.Name] {
			continue
		}
		for _, device := range slice.Spec.Devices {
			attr, ok := device.Attributes[attrKey]
			if !ok || attr.StringValue == nil || *attr.StringValue != resourceAttribute {
				continue
			}
			count++
		}
	}
	return count, nil
}

func draSliceForNode(slice *resourceapi.ResourceSlice, nodeName string) bool {
	return slice.Spec.Driver == consts.DRADriverBaseDeviceClassName &&
		slice.Spec.NodeName != nil &&
		*slice.Spec.NodeName == nodeName
}

func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func cleanDRAClaims(ctx context.Context, namespace string) {
	// Best effort: this runs during teardown, so failures are logged rather than asserted.
	logErr := func(action string, err error) {
		if err != nil && !errors.IsNotFound(err) {
			fmt.Fprintf(GinkgoWriter, "DRA cleanup: %s in namespace %s failed: %v\n", action, namespace, err)
		}
	}

	logErr("deleting pods", namespaces.CleanPods(namespace, clients))

	claimList := &resourceapi.ResourceClaimList{}
	err := clients.List(ctx, claimList, runtimeclient.InNamespace(namespace))
	logErr("listing ResourceClaims", err)
	for i := range claimList.Items {
		logErr("deleting ResourceClaim "+claimList.Items[i].Name, clients.Delete(ctx, &claimList.Items[i]))
	}

	templateList := &resourceapi.ResourceClaimTemplateList{}
	err = clients.List(ctx, templateList, runtimeclient.InNamespace(namespace))
	logErr("listing ResourceClaimTemplates", err)
	for i := range templateList.Items {
		logErr("deleting ResourceClaimTemplate "+templateList.Items[i].Name, clients.Delete(ctx, &templateList.Items[i]))
	}
}

// deleteAndWait deletes obj (identified by its name and namespace) and waits until it is gone.
// A missing object is not an error.
func deleteAndWait(ctx context.Context, obj runtimeclient.Object, opts ...runtimeclient.DeleteOption) {
	key := runtimeclient.ObjectKeyFromObject(obj)
	Expect(runtimeclient.IgnoreNotFound(clients.Delete(ctx, obj, opts...))).To(Succeed())
	Eventually(ctx, func(g Gomega) {
		err := clients.Get(ctx, key, obj)
		g.Expect(errors.IsNotFound(err)).To(BeTrue(), "%T %s still exists (err: %v)", obj, key, err)
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
}

func deletePodSync(ctx context.Context, namespace, name string) {
	deleteAndWait(ctx,
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}},
		runtimeclient.GracePeriodSeconds(0))
}

func deleteClaimSync(ctx context.Context, namespace, name string) {
	deleteAndWait(ctx, &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}})
}

func deleteClaimTemplateSync(ctx context.Context, namespace, name string) {
	deleteAndWait(ctx, &resourceapi.ResourceClaimTemplate{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}})
}

func deletePolicySync(ctx context.Context, name string) {
	deleteAndWait(ctx, &sriovv1.SriovNetworkNodePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: name}})
}

// draResourcePrefix returns the resource prefix the operator was deployed with
// (RESOURCE_PREFIX, as exported by hack/env.sh), defaulting to "openshift.io".
func draResourcePrefix() string {
	if prefix := os.Getenv("RESOURCE_PREFIX"); prefix != "" {
		return prefix
	}
	return "openshift.io"
}

func isOperatorWebhookEnabled(ctx context.Context) bool {
	cfg := &sriovv1.SriovOperatorConfig{}
	Expect(clients.Get(ctx, runtimeclient.ObjectKey{Name: "default", Namespace: operatorNamespace}, cfg)).To(Succeed())
	return cfg.Spec.EnableOperatorWebhook
}

// draResourceNameSelector returns a CEL expression selecting devices by SR-IOV resource name.
func draResourceNameSelector(resourceName string) string {
	return fmt.Sprintf(`device.attributes["k8s.cni.cncf.io"].resourceName == "%s"`, resourceName)
}

// newDRAClaimSpec builds a ResourceClaimSpec with a single request for count devices of
// deviceClassName, optionally narrowed by a CEL expression.
func newDRAClaimSpec(deviceClassName string, count int64, celExpression string) resourceapi.ResourceClaimSpec {
	request := &resourceapi.ExactDeviceRequest{
		DeviceClassName: deviceClassName,
		Count:           count,
	}
	if celExpression != "" {
		request.Selectors = []resourceapi.DeviceSelector{
			{CEL: &resourceapi.CELDeviceSelector{Expression: celExpression}},
		}
	}
	return resourceapi.ResourceClaimSpec{
		Devices: resourceapi.DeviceClaim{
			Requests: []resourceapi.DeviceRequest{{Name: "req-device", Exactly: request}},
		},
	}
}

func createDRAClaim(ctx context.Context, name, deviceClassName string, count int64, celExpression string) {
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespaces.Test},
		Spec:       newDRAClaimSpec(deviceClassName, count, celExpression),
	}
	Expect(clients.Create(ctx, claim)).To(Succeed())
}

// createDRAPod creates a test pod pinned to node whose container consumes the given claim
// source (a ResourceClaim or a ResourceClaimTemplate) under the draClaimRefName reference.
func createDRAPod(ctx context.Context, name, node string, claimSource corev1.PodResourceClaim) *corev1.Pod {
	claimSource.Name = draClaimRefName
	podObj := pod.GetDefinition()
	podObj.Name = name
	podObj.GenerateName = ""
	podObj.Namespace = namespaces.Test
	podObj.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": node}
	podObj.Spec.ResourceClaims = []corev1.PodResourceClaim{claimSource}
	podObj.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: draClaimRefName}}
	created, err := clients.Pods(namespaces.Test).Create(ctx, podObj, metav1.CreateOptions{})
	Expect(err).ToNot(HaveOccurred())
	return created
}

func isClaimReservedFor(claim *resourceapi.ResourceClaim, uid types.UID) bool {
	for _, res := range claim.Status.ReservedFor {
		if res.UID == uid {
			return true
		}
	}
	return false
}

// waitForDRAClaimAllocated waits until the claim is allocated count distinct devices from the
// SR-IOV DRA driver.
func waitForDRAClaimAllocated(ctx context.Context, name string, count int) {
	Eventually(ctx, func(g Gomega) {
		c := &resourceapi.ResourceClaim{}
		g.Expect(clients.Get(ctx, runtimeclient.ObjectKey{Namespace: namespaces.Test, Name: name}, c)).To(Succeed())
		g.Expect(c.Status.Allocation).ToNot(BeNil())
		g.Expect(c.Status.Allocation.Devices.Results).To(HaveLen(count))
		devices := map[string]struct{}{}
		for _, result := range c.Status.Allocation.Devices.Results {
			g.Expect(result.Driver).To(Equal(consts.DRADriverBaseDeviceClassName))
			g.Expect(result.Device).ToNot(BeEmpty())
			g.Expect(result.Pool).ToNot(BeEmpty())
			devices[result.Pool+"/"+result.Device] = struct{}{}
		}
		g.Expect(devices).To(HaveLen(count), "Allocated devices must be distinct")
	}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())
}

func waitForDRAClaimReservedFor(ctx context.Context, name string, podUID types.UID) {
	Eventually(ctx, func(g Gomega) {
		c := &resourceapi.ResourceClaim{}
		g.Expect(clients.Get(ctx, runtimeclient.ObjectKey{Namespace: namespaces.Test, Name: name}, c)).To(Succeed())
		g.Expect(isClaimReservedFor(c, podUID)).To(BeTrue(), "ResourceClaim %s not reserved for pod %s", name, podUID)
	}).WithTimeout(1 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
}

func waitForPodRunningOnNode(ctx context.Context, name, node string) *corev1.Pod {
	var running *corev1.Pod
	Eventually(ctx, func(g Gomega) {
		p, err := clients.Pods(namespaces.Test).Get(ctx, name, metav1.GetOptions{})
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(p.Spec.NodeName).To(Equal(node))
		g.Expect(p.Status.Phase).To(Equal(corev1.PodRunning))
		running = p
	}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())
	return running
}

// expectDRAPodUnschedulable waits until the scheduler has reported the pod as not schedulable
// (so the check below is not trivially true for a pod it has not looked at yet), then verifies
// the pod stays Pending and its claim stays unallocated.
func expectDRAPodUnschedulable(ctx context.Context, podName, claimName string) {
	Eventually(ctx, func(g Gomega) {
		p, err := clients.Pods(namespaces.Test).Get(ctx, podName, metav1.GetOptions{})
		g.Expect(err).ToNot(HaveOccurred())
		rejected := false
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
				rejected = true
			}
		}
		g.Expect(rejected).To(BeTrue(), "scheduler has not reported pod %s as unschedulable: %v", podName, p.Status.Conditions)
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())

	Consistently(ctx, func(g Gomega) {
		c := &resourceapi.ResourceClaim{}
		g.Expect(clients.Get(ctx, runtimeclient.ObjectKey{Namespace: namespaces.Test, Name: claimName}, c)).To(Succeed())
		g.Expect(c.Status.Allocation).To(BeNil())

		p, err := clients.Pods(namespaces.Test).Get(ctx, podName, metav1.GetOptions{})
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(p.Spec.NodeName).To(BeEmpty())
		g.Expect(p.Status.Phase).To(Equal(corev1.PodPending))
	}).WithTimeout(15 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
}

// Ordered is required: later specs depend on earlier ones (enable DRA => create
// SriovNetworkNodePolicy / unblock init => ResourceSlices publish initial VFs =>
// NumVfs change updates ResourceSlices => pods allocate the VFs through
// ResourceClaims / ResourceClaimTemplates => a second policy is created and deleted
// while DRA stays active => disable and clean up).
var _ = Describe("[sriov] DRA", Ordered, func() {
	var (
		node             string
		nic              *sriovv1.InterfaceExt
		policyName       string
		deviceClassName  string
		deviceAttrsName  string
		extendedResource string
	)

	BeforeAll(func() {
		if platformType != consts.Baremetal {
			Skip("DRA operator tests require a baremetal (or emulated-PF) platform")
		}
		if discovery.Enabled() {
			// These specs toggle the feature gate, create SriovNetworkNodePolicies and
			// consume every VF of the selected PF.
			Skip("DRA operator tests are unsuitable to be run in discovery mode")
		}
		available, err := isDeviceClassAPIAvailable()
		Expect(err).ToNot(HaveOccurred())
		if !available {
			Skip("resource.k8s.io/v1 DeviceClass API is not available on this cluster")
		}

		err = namespaces.Create(namespaces.Test, clients)
		Expect(err).ToNot(HaveOccurred())

		err = namespaces.Clean(operatorNamespace, namespaces.Test, clients, discovery.Enabled())
		Expect(err).ToNot(HaveOccurred())

		featureFlagInitialValue := isFeatureFlagEnabled(consts.DynamicResourceAllocationFeatureGate)
		DeferCleanup(func(ctx context.Context) {
			By("Cleaning DRA claims and templates in test namespace")
			cleanDRAClaims(ctx, namespaces.Test)
			By("Restoring dynamicResourceAllocation feature flag and cleaning test state")
			setFeatureFlag(consts.DynamicResourceAllocationFeatureGate, featureFlagInitialValue)
			WaitForSRIOVStable()
			Expect(namespaces.Clean(operatorNamespace, namespaces.Test, clients, discovery.Enabled())).To(Succeed())
		})

		// Assign the package-level sriovInfos (no :=) so later specs use this discovery.
		sriovInfos, err = cluster.DiscoverSriov(clients, operatorNamespace)
		Expect(err).ToNot(HaveOccurred())
		node, nic, err = sriovInfos.FindOneSriovNodeAndDevice()
		Expect(err).ToNot(HaveOccurred())
		By(fmt.Sprintf("Using device %s on node %s", nic.Name, node))

		deviceClassName = dra.ResourceNameToDeviceClassName(draResourceName)
		deviceAttrsName = deviceClassName + "-attrs"
		extendedResource = draResourcePrefix() + "/" + draResourceName

		// Enable DRA before creating a SriovNetworkNodePolicy. Nodes with no
		// interfaces are unblocked, so the DRA driver leaves Init without a policy.
		By("Enabling dynamicResourceAllocation feature flag")
		setFeatureFlag(consts.DynamicResourceAllocationFeatureGate, true)
	})

	It("should deploy the DRA driver DaemonSet and remove the device plugin", func(ctx SpecContext) {
		By("Waiting for sriov-dra-driver DaemonSet to exist")
		Eventually(ctx, func(g Gomega) {
			ds := &appsv1.DaemonSet{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      consts.DRADriverDaemonSetName,
			}, ds)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(ds.Status.DesiredNumberScheduled).To(BeNumerically(">", 0))
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for DRA driver pods to become Ready")
		Eventually(ctx, func(g Gomega) {
			pods, err := clients.Pods(operatorNamespace).List(ctx, metav1.ListOptions{
				LabelSelector: draDriverAppLabel,
			})
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(pods.Items).ToNot(BeEmpty())
			for _, p := range pods.Items {
				g.Expect(draDriverInitContainersTerminated(&p)).To(BeTrue(),
					"DRA driver pod %s still in Init: %v", p.Name, p.Status.InitContainerStatuses)
				g.Expect(p.Status.Phase).To(Equal(corev1.PodRunning))
				g.Expect(isPodReady(&p)).To(BeTrue(), "DRA driver pod %s is not Ready", p.Name)
			}
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for sriov-device-plugin DaemonSet to be removed")
		assertObjectIsNotFound(devicePluginDSName, &appsv1.DaemonSet{})
	})

	It("should create the base DeviceClass managed by the operator", func(ctx SpecContext) {
		Eventually(ctx, func(g Gomega) {
			dc := &resourceapi.DeviceClass{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Name: consts.DRADriverBaseDeviceClassName,
			}, dc)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(dc.Labels).To(HaveKeyWithValue(dra.GeneratedByLabel, consts.SriovNetworkOperatorIdentifier))
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	})

	It("should create SriovNetworkNodePolicy and unblock DRA driver init with DRA CRs", func(ctx SpecContext) {
		By("Creating SriovNetworkNodePolicy for resource " + draResourceName)
		created, err := network.CreateSriovPolicy(clients, "test-dra-policy-", operatorNamespace, nic.Name, node, initialVFs, draResourceName, "netdevice")
		Expect(err).ToNot(HaveOccurred())
		policyName = created.Name
		WaitForSRIOVStable()

		By("Waiting for DRA driver pods to leave Init and become Running")
		Eventually(ctx, func(g Gomega) {
			pods, err := clients.Pods(operatorNamespace).List(ctx, metav1.ListOptions{
				LabelSelector: draDriverAppLabel,
			})
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(pods.Items).ToNot(BeEmpty())

			readyPods := 0
			for _, p := range pods.Items {
				g.Expect(draDriverInitContainersTerminated(&p)).To(BeTrue(),
					"DRA driver pod %s still has incomplete init containers: %v", p.Name, p.Status.InitContainerStatuses)
				g.Expect(p.Status.Phase).To(Equal(corev1.PodRunning),
					"DRA driver pod %s phase=%s", p.Name, p.Status.Phase)
				if isPodReady(&p) {
					readyPods++
				}
			}
			g.Expect(readyPods).To(BeNumerically(">", 0))
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for sriov-dra-driver DaemonSet to finish rollout")
		Eventually(ctx, func(g Gomega) {
			ds := &appsv1.DaemonSet{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      consts.DRADriverDaemonSetName,
			}, ds)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(ds.Status.ObservedGeneration).To(BeNumerically(">=", ds.Generation))
			g.Expect(ds.Status.NumberReady).To(Equal(ds.Status.DesiredNumberScheduled))
			g.Expect(ds.Status.UpdatedNumberScheduled).To(Equal(ds.Status.DesiredNumberScheduled))
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for DeviceAttributes " + deviceAttrsName)
		Eventually(ctx, func(g Gomega) {
			attrs := &sriovdrav1alpha1.DeviceAttributes{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      deviceAttrsName,
			}, attrs)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(attrs.Labels).To(HaveKeyWithValue(dra.GeneratedByLabel, consts.SriovNetworkOperatorIdentifier))
			g.Expect(attrs.Labels).To(HaveKeyWithValue(dra.ResourcePoolLabel, deviceClassName))
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for SriovResourcePolicy for node " + node)
		Eventually(ctx, func(g Gomega) {
			policy := &sriovdrav1alpha1.SriovResourcePolicy{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      node,
			}, policy)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(policy.Labels).To(HaveKeyWithValue(dra.GeneratedByLabel, consts.SriovNetworkOperatorIdentifier))
			g.Expect(policy.Labels).To(HaveKeyWithValue(dra.SriovResourcePolicyNodeLabel, node))
			g.Expect(policy.Spec.Configs).ToNot(BeEmpty())
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for per-resource DeviceClass " + deviceClassName)
		Eventually(ctx, func(g Gomega) {
			dc := &resourceapi.DeviceClass{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{Name: deviceClassName}, dc)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(dc.Labels).To(HaveKeyWithValue(dra.GeneratedByLabel, consts.SriovNetworkOperatorIdentifier))
			g.Expect(dc.Labels).To(HaveKeyWithValue(dra.DeviceClassResourceNameLabel, draResourceName))
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	})

	It("should reject policies whose resourceName collides after DRA normalization via admission webhook", func(ctx SpecContext) {
		if !isOperatorWebhookEnabled(ctx) {
			Skip("Operator admission webhook is disabled (enableOperatorWebhook=false)")
		}

		collidingResourceName := strings.ToUpper(draResourceName)
		collidingPolicy := &sriovv1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dra-colliding-policy",
				Namespace: operatorNamespace,
			},
			Spec: sriovv1.SriovNetworkNodePolicySpec{
				NodeSelector: map[string]string{
					"kubernetes.io/hostname": node,
				},
				NumVfs:       initialVFs,
				ResourceName: collidingResourceName,
				Priority:     99,
				NicSelector: sriovv1.SriovNetworkNicSelector{
					PfNames: []string{nic.Name},
				},
				DeviceType: "netdevice",
			},
		}

		DeferCleanup(func(cleanupCtx context.Context) {
			// Only reached with an existing policy when the webhook wrongly admitted it:
			// remove it and let the nodes settle before the next spec.
			err := clients.Delete(cleanupCtx, collidingPolicy)
			if err == nil {
				WaitForSRIOVStable()
				return
			}
			Expect(runtimeclient.IgnoreNotFound(err)).To(Succeed())
		})

		By("Attempting to create a policy with resourceName " + collidingResourceName + " colliding with " + draResourceName)
		err := clients.Create(ctx, collidingPolicy)
		Expect(err).To(HaveOccurred(), "Admission webhook must reject colliding resourceName %s", collidingResourceName)
		Expect(err.Error()).To(ContainSubstring("normalizes to the same DRA device class name"))
	})

	It("should publish the initial VFs on ResourceSlices for the selected node", func(ctx SpecContext) {
		By("Waiting for ResourceSlices to publish the initial VFs on " + node)
		Eventually(ctx, func(g Gomega) {
			count, err := countAdvertisedDRADevices(ctx, node, extendedResource)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(count).To(Equal(initialVFs))
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Checking other nodes do not advertise this resource")
		for _, p := range listDRADriverPods(ctx) {
			if p.Spec.NodeName == "" || p.Spec.NodeName == node {
				continue
			}
			count, err := countAdvertisedDRADevices(ctx, p.Spec.NodeName, extendedResource)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(0),
				"node %s advertised %s before a policy selected it", p.Spec.NodeName, extendedResource)
		}
	})

	It("should publish the additional VFs on ResourceSlices when policy interfaces change", func(ctx SpecContext) {
		otherCounts := map[string]int{}
		for _, p := range listDRADriverPods(ctx) {
			if p.Spec.NodeName != "" && p.Spec.NodeName != node {
				count, err := countAdvertisedDRADevices(ctx, p.Spec.NodeName, extendedResource)
				Expect(err).ToNot(HaveOccurred())
				otherCounts[p.Spec.NodeName] = count
			}
		}

		By("Changing NumVfs so SriovNetworkNodeState interfaces are updated")
		Eventually(ctx, func(g Gomega) {
			policy := &sriovv1.SriovNetworkNodePolicy{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      policyName,
			}, policy)
			g.Expect(err).ToNot(HaveOccurred())
			policy.Spec.NumVfs = updatedVFs
			g.Expect(clients.Update(ctx, policy)).To(Succeed())
		}).WithTimeout(1 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
		WaitForSRIOVStable()

		By(fmt.Sprintf("Waiting for ResourceSlice devices on %s to increase from %d to %d", node, initialVFs, updatedVFs))
		Eventually(ctx, func(g Gomega) {
			count, err := countAdvertisedDRADevices(ctx, node, extendedResource)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(count).To(Equal(updatedVFs))
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Checking ResourceSlice device counts on other nodes did not change")
		for nodeName, before := range otherCounts {
			count, err := countAdvertisedDRADevices(ctx, nodeName, extendedResource)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(before),
				"ResourceSlice devices changed on %s", nodeName)
		}
	})

	It("should allocate SR-IOV resources to a pod via ResourceClaim using the per-resource DeviceClass", func(ctx SpecContext) {
		claimName := "test-dra-claim"
		podName := "test-dra-claim-pod"

		DeferCleanup(func(cleanupCtx context.Context) {
			deletePodSync(cleanupCtx, namespaces.Test, podName)
			deleteClaimSync(cleanupCtx, namespaces.Test, claimName)
		})

		By("Creating a ResourceClaim for DeviceClass " + deviceClassName)
		createDRAClaim(ctx, claimName, deviceClassName, 1, "")

		By("Creating a pod consuming the ResourceClaim")
		createdPod := createDRAPod(ctx, podName, node, corev1.PodResourceClaim{ResourceClaimName: ptr.To(claimName)})

		By("Waiting for the ResourceClaim to be allocated")
		waitForDRAClaimAllocated(ctx, claimName, 1)

		By("Verifying the ResourceClaim is reserved for the pod")
		waitForDRAClaimReservedFor(ctx, claimName, createdPod.UID)

		By("Waiting for the pod to be Running on " + node)
		waitForPodRunningOnNode(ctx, podName, node)

		By("Deleting the pod and verifying reservation is released")
		deletePodSync(ctx, namespaces.Test, podName)

		Eventually(ctx, func(g Gomega) {
			c := &resourceapi.ResourceClaim{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{Namespace: namespaces.Test, Name: claimName}, c)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(isClaimReservedFor(c, createdPod.UID)).To(BeFalse(),
				"ResourceClaim %s still reserved for deleted pod %s", claimName, createdPod.UID)
		}).WithTimeout(1 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
	})

	It("should allocate SR-IOV resources to a pod via ResourceClaimTemplate", func(ctx SpecContext) {
		templateName := "test-dra-template"
		podName := "test-dra-template-pod"

		DeferCleanup(func(cleanupCtx context.Context) {
			deletePodSync(cleanupCtx, namespaces.Test, podName)
			deleteClaimTemplateSync(cleanupCtx, namespaces.Test, templateName)
		})

		By("Creating a ResourceClaimTemplate for DeviceClass " + deviceClassName)
		template := &resourceapi.ResourceClaimTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      templateName,
				Namespace: namespaces.Test,
			},
			Spec: resourceapi.ResourceClaimTemplateSpec{
				Spec: newDRAClaimSpec(deviceClassName, 1, ""),
			},
		}
		Expect(clients.Create(ctx, template)).To(Succeed())

		By("Creating a pod referencing the ResourceClaimTemplate")
		createdPod := createDRAPod(ctx, podName, node, corev1.PodResourceClaim{ResourceClaimTemplateName: ptr.To(templateName)})

		By("Waiting for the pod to be Running and checking generated ResourceClaim")
		var generatedClaimName string
		Eventually(ctx, func(g Gomega) {
			p, err := clients.Pods(namespaces.Test).Get(ctx, podName, metav1.GetOptions{})
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(p.Spec.NodeName).To(Equal(node))
			g.Expect(p.Status.Phase).To(Equal(corev1.PodRunning))
			name := ""
			for _, status := range p.Status.ResourceClaimStatuses {
				if status.Name == draClaimRefName && status.ResourceClaimName != nil {
					name = *status.ResourceClaimName
				}
			}
			g.Expect(name).ToNot(BeEmpty(), "pod %s has no generated ResourceClaim for %s: %v",
				podName, draClaimRefName, p.Status.ResourceClaimStatuses)
			generatedClaimName = name
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Verifying generated ResourceClaim " + generatedClaimName + " is allocated and reserved for the pod")
		waitForDRAClaimAllocated(ctx, generatedClaimName, 1)
		waitForDRAClaimReservedFor(ctx, generatedClaimName, createdPod.UID)

		By("Deleting the pod and verifying generated ResourceClaim is garbage-collected")
		deletePodSync(ctx, namespaces.Test, podName)

		Eventually(ctx, func(g Gomega) {
			c := &resourceapi.ResourceClaim{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{Namespace: namespaces.Test, Name: generatedClaimName}, c)
			g.Expect(errors.IsNotFound(err)).To(BeTrue(), "generated ResourceClaim %s still exists", generatedClaimName)
		}).WithTimeout(1 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
	})

	It("should allocate SR-IOV resources using the base DeviceClass with a CEL selector", func(ctx SpecContext) {
		claimName := "test-dra-base-claim"
		podName := "test-dra-base-pod"

		DeferCleanup(func(cleanupCtx context.Context) {
			deletePodSync(cleanupCtx, namespaces.Test, podName)
			deleteClaimSync(cleanupCtx, namespaces.Test, claimName)
		})

		By("Creating a ResourceClaim referencing base DeviceClass with CEL selector")
		createDRAClaim(ctx, claimName, consts.DRADriverBaseDeviceClassName, 1, draResourceNameSelector(extendedResource))

		By("Creating a pod consuming the base DeviceClass ResourceClaim")
		createDRAPod(ctx, podName, node, corev1.PodResourceClaim{ResourceClaimName: ptr.To(claimName)})

		By("Waiting for the pod to be Running and claim allocated")
		waitForPodRunningOnNode(ctx, podName, node)
		waitForDRAClaimAllocated(ctx, claimName, 1)
	})

	It("should not schedule a pod when a CEL selector matches no available devices", func(ctx SpecContext) {
		claimName := "test-dra-unmatched-cel-claim"
		podName := "test-dra-unmatched-cel-pod"

		DeferCleanup(func(cleanupCtx context.Context) {
			deletePodSync(cleanupCtx, namespaces.Test, podName)
			deleteClaimSync(cleanupCtx, namespaces.Test, claimName)
		})

		By("Creating a ResourceClaim referencing base DeviceClass with an unmatchable CEL selector")
		createDRAClaim(ctx, claimName, consts.DRADriverBaseDeviceClassName, 1, draResourceNameSelector("nonexistent-resource"))

		By("Creating a pod consuming the unmatchable ResourceClaim")
		createDRAPod(ctx, podName, node, corev1.PodResourceClaim{ResourceClaimName: ptr.To(claimName)})

		By("Verifying the claim remains unallocated and pod remains Pending")
		expectDRAPodUnschedulable(ctx, podName, claimName)
	})

	It("should not schedule a pod when DRA resource requests exceed available capacity", func(ctx SpecContext) {
		claimName := "test-dra-overcommit-claim"
		podName := "test-dra-overcommit-pod"

		DeferCleanup(func(cleanupCtx context.Context) {
			deletePodSync(cleanupCtx, namespaces.Test, podName)
			deleteClaimSync(cleanupCtx, namespaces.Test, claimName)
		})

		By("Creating a ResourceClaim requesting more devices than available on the node")
		createDRAClaim(ctx, claimName, deviceClassName, int64(updatedVFs+1), "")

		By("Creating a pod referencing the overcommitted ResourceClaim")
		createDRAPod(ctx, podName, node, corev1.PodResourceClaim{ResourceClaimName: ptr.To(claimName)})

		By("Verifying the claim remains unallocated and pod remains Pending")
		expectDRAPodUnschedulable(ctx, podName, claimName)
	})

	It("should allocate multiple SR-IOV devices to a single pod via ResourceClaim", func(ctx SpecContext) {
		claimName := "test-dra-multi-device-claim"
		podName := "test-dra-multi-device-pod"

		DeferCleanup(func(cleanupCtx context.Context) {
			deletePodSync(cleanupCtx, namespaces.Test, podName)
			deleteClaimSync(cleanupCtx, namespaces.Test, claimName)
		})

		By("Creating a ResourceClaim requesting 2 devices for DeviceClass " + deviceClassName)
		createDRAClaim(ctx, claimName, deviceClassName, 2, "")

		By("Creating a pod consuming the multi-device ResourceClaim")
		createdPod := createDRAPod(ctx, podName, node, corev1.PodResourceClaim{ResourceClaimName: ptr.To(claimName)})

		By("Waiting for the ResourceClaim to allocate 2 distinct devices")
		waitForDRAClaimAllocated(ctx, claimName, 2)

		By("Verifying the ResourceClaim is reserved for the pod")
		waitForDRAClaimReservedFor(ctx, claimName, createdPod.UID)

		By("Waiting for the pod to be Running on " + node)
		waitForPodRunningOnNode(ctx, podName, node)
	})

	It("should enforce capacity limits across multiple pods sharing the resource pool", func(ctx SpecContext) {
		claimName1 := "test-dra-shared-claim-1"
		claimName2 := "test-dra-shared-claim-2"
		claimName3 := "test-dra-shared-claim-3"
		podName1 := "test-dra-shared-pod-1"
		podName2 := "test-dra-shared-pod-2"
		podName3 := "test-dra-shared-pod-3"

		DeferCleanup(func(cleanupCtx context.Context) {
			deletePodSync(cleanupCtx, namespaces.Test, podName3)
			deletePodSync(cleanupCtx, namespaces.Test, podName2)
			deletePodSync(cleanupCtx, namespaces.Test, podName1)
			deleteClaimSync(cleanupCtx, namespaces.Test, claimName3)
			deleteClaimSync(cleanupCtx, namespaces.Test, claimName2)
			deleteClaimSync(cleanupCtx, namespaces.Test, claimName1)
		})

		By("Creating first pod claiming 2 devices")
		createDRAClaim(ctx, claimName1, deviceClassName, 2, "")
		createDRAPod(ctx, podName1, node, corev1.PodResourceClaim{ResourceClaimName: ptr.To(claimName1)})
		waitForPodRunningOnNode(ctx, podName1, node)

		By(fmt.Sprintf("Creating second pod claiming 2 devices (exhausting all %d node VFs)", updatedVFs))
		createDRAClaim(ctx, claimName2, deviceClassName, 2, "")
		createDRAPod(ctx, podName2, node, corev1.PodResourceClaim{ResourceClaimName: ptr.To(claimName2)})
		waitForPodRunningOnNode(ctx, podName2, node)

		By("Creating third pod requesting 1 device when no capacity is left")
		createDRAClaim(ctx, claimName3, deviceClassName, 1, "")
		createDRAPod(ctx, podName3, node, corev1.PodResourceClaim{ResourceClaimName: ptr.To(claimName3)})
		expectDRAPodUnschedulable(ctx, podName3, claimName3)

		By("Deleting first pod to free up capacity for the pending third pod")
		deletePodSync(ctx, namespaces.Test, podName1)
		deleteClaimSync(ctx, namespaces.Test, claimName1)

		By("Verifying third pod is now allocated and transitions to Running")
		waitForDRAClaimAllocated(ctx, claimName3, 1)
		waitForPodRunningOnNode(ctx, podName3, node)
	})

	// Note: End-to-end pod network attachment with data-plane verification (traffic/ping on net1)
	// will be added once Multus CNI DRA support (k8snetworkplumbingwg/multus-cni#1455) is available in CI.

	It("should clean up DeviceAttributes and DeviceClass when a policy is deleted while DRA remains active", func(ctx SpecContext) {
		var secondNode string
		var secondNic *sriovv1.InterfaceExt
		for _, nName := range sriovInfos.Nodes {
			devs, err := sriovInfos.FindSriovDevices(nName)
			if err != nil {
				continue
			}
			for _, d := range devs {
				if nName != node || d.PciAddress != nic.PciAddress {
					secondNode = nName
					secondNic = d
					break
				}
			}
			if secondNic != nil {
				break
			}
		}
		if secondNic == nil {
			Skip("Test requires at least two SR-IOV devices across the cluster")
		}

		secondResource := "seconddraresource"
		secondDCName := dra.ResourceNameToDeviceClassName(secondResource)
		secondAttrsName := secondDCName + "-attrs"

		By("Creating secondary policy for resource " + secondResource + " on " + secondNode)
		created, err := network.CreateSriovPolicy(clients, "test-dra-second-policy-", operatorNamespace, secondNic.Name, secondNode, 2, secondResource, "netdevice")
		Expect(err).ToNot(HaveOccurred())
		secondPolicyName := created.Name

		DeferCleanup(func(cleanupCtx context.Context) {
			deletePolicySync(cleanupCtx, secondPolicyName)
			WaitForSRIOVStable()
		})

		WaitForSRIOVStable()

		By("Waiting for secondary DeviceAttributes " + secondAttrsName + " to be created")
		Eventually(ctx, func(g Gomega) {
			attrs := &sriovdrav1alpha1.DeviceAttributes{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      secondAttrsName,
			}, attrs)
			g.Expect(err).ToNot(HaveOccurred())
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for secondary DeviceClass " + secondDCName + " to be created")
		Eventually(ctx, func(g Gomega) {
			dc := &resourceapi.DeviceClass{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{Name: secondDCName}, dc)
			g.Expect(err).ToNot(HaveOccurred())
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Deleting secondary policy " + secondPolicyName + " while DRA is still active")
		deletePolicySync(ctx, secondPolicyName)
		WaitForSRIOVStable()

		By("Waiting for secondary DeviceAttributes " + secondAttrsName + " to be removed")
		Eventually(ctx, func(g Gomega) {
			attrs := &sriovdrav1alpha1.DeviceAttributes{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      secondAttrsName,
			}, attrs)
			g.Expect(errors.IsNotFound(err)).To(BeTrue(), "DeviceAttributes %s still exists", secondAttrsName)
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for secondary DeviceClass " + secondDCName + " to be removed")
		Eventually(ctx, func(g Gomega) {
			dc := &resourceapi.DeviceClass{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{Name: secondDCName}, dc)
			g.Expect(errors.IsNotFound(err)).To(BeTrue(), "DeviceClass %s still exists", secondDCName)
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Verifying primary policy's DeviceClass " + deviceClassName + " is still present")
		primaryDC := &resourceapi.DeviceClass{}
		Expect(clients.Get(ctx, runtimeclient.ObjectKey{Name: deviceClassName}, primaryDC)).To(Succeed())

		By("Verifying primary policy's DeviceAttributes " + deviceAttrsName + " is still present")
		primaryAttrs := &sriovdrav1alpha1.DeviceAttributes{}
		Expect(clients.Get(ctx, runtimeclient.ObjectKey{
			Namespace: operatorNamespace,
			Name:      deviceAttrsName,
		}, primaryAttrs)).To(Succeed())
	})

	It("should clean up DRA objects and restore the device plugin when DRA is disabled", func(ctx SpecContext) {
		By("Disabling dynamicResourceAllocation feature flag")
		setFeatureFlag(consts.DynamicResourceAllocationFeatureGate, false)
		WaitForSRIOVStable()

		By("Waiting for sriov-dra-driver DaemonSet to be removed")
		assertObjectIsNotFound(consts.DRADriverDaemonSetName, &appsv1.DaemonSet{})

		By("Waiting for operator-generated DeviceAttributes to be removed")
		Eventually(ctx, func(g Gomega) {
			list := &sriovdrav1alpha1.DeviceAttributesList{}
			err := clients.List(ctx, list,
				runtimeclient.InNamespace(operatorNamespace),
				runtimeclient.MatchingLabels(dra.OperatorGeneratedByLabels()))
			if meta.IsNoMatchError(err) {
				return
			}
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(list.Items).To(BeEmpty())
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for operator-generated SriovResourcePolicy to be removed")
		Eventually(ctx, func(g Gomega) {
			list := &sriovdrav1alpha1.SriovResourcePolicyList{}
			err := clients.List(ctx, list,
				runtimeclient.InNamespace(operatorNamespace),
				runtimeclient.MatchingLabels(dra.OperatorGeneratedByLabels()))
			if meta.IsNoMatchError(err) {
				return
			}
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(list.Items).To(BeEmpty())
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for operator-generated DeviceClasses to be removed")
		Eventually(ctx, func(g Gomega) {
			list := &resourceapi.DeviceClassList{}
			err := clients.List(ctx, list,
				runtimeclient.MatchingLabels(dra.OperatorGeneratedByLabels()))
			if meta.IsNoMatchError(err) {
				return
			}
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(list.Items).To(BeEmpty())
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for base DeviceClass to be removed")
		Eventually(ctx, func(g Gomega) {
			dc := &resourceapi.DeviceClass{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Name: consts.DRADriverBaseDeviceClassName,
			}, dc)
			g.Expect(errors.IsNotFound(err) || meta.IsNoMatchError(err)).To(BeTrue())
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for sriov-device-plugin DaemonSet to be restored")
		Eventually(ctx, func(g Gomega) {
			ds := &appsv1.DaemonSet{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      devicePluginDSName,
			}, ds)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(ds.Status.NumberReady).To(Equal(ds.Status.DesiredNumberScheduled))
			g.Expect(ds.Status.DesiredNumberScheduled).To(BeNumerically(">", 0))
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for at least one Running device-plugin pod")
		Eventually(ctx, func(g Gomega) {
			pods, err := clients.Pods(operatorNamespace).List(ctx, metav1.ListOptions{
				LabelSelector: devicePluginAppLabel,
			})
			g.Expect(err).ToNot(HaveOccurred())
			running := 0
			for _, p := range pods.Items {
				if p.Status.Phase == corev1.PodRunning {
					running++
				}
			}
			g.Expect(running).To(BeNumerically(">", 0))
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for device-plugin-config ConfigMap to be restored")
		Eventually(ctx, func(g Gomega) {
			cfg := &corev1.ConfigMap{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{
				Namespace: operatorNamespace,
				Name:      consts.ConfigMapName,
			}, cfg)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cfg.Data).To(HaveKeyWithValue(node, ContainSubstring(draResourceName)))
		}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

		By("Waiting for node " + node + " to advertise allocatable " + extendedResource)
		Eventually(ctx, func(g Gomega) {
			n := &corev1.Node{}
			err := clients.Get(ctx, runtimeclient.ObjectKey{Name: node}, n)
			g.Expect(err).ToNot(HaveOccurred())
			qty, exists := n.Status.Allocatable[corev1.ResourceName(extendedResource)]
			g.Expect(exists).To(BeTrue(), "Node %s does not advertise %s", node, extendedResource)
			g.Expect(qty.Value()).To(BeNumerically("==", updatedVFs))
		}).WithTimeout(waitingTime).WithPolling(5 * time.Second).Should(Succeed())

		By("Verifying device plugin allocates resources to a pod after rollback")
		dpPodName := "test-dra-rollback-dp-pod"
		DeferCleanup(func(cleanupCtx context.Context) {
			deletePodSync(cleanupCtx, namespaces.Test, dpPodName)
		})

		dpPodObj := pod.GetDefinition()
		dpPodObj.Name = dpPodName
		dpPodObj.GenerateName = ""
		dpPodObj.Namespace = namespaces.Test
		dpPodObj.Spec.NodeSelector = map[string]string{
			"kubernetes.io/hostname": node,
		}
		if dpPodObj.Spec.Containers[0].Resources.Limits == nil {
			dpPodObj.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
		}
		dpPodObj.Spec.Containers[0].Resources.Limits[corev1.ResourceName(extendedResource)] = resource.MustParse("1")
		if dpPodObj.Spec.Containers[0].Resources.Requests == nil {
			dpPodObj.Spec.Containers[0].Resources.Requests = corev1.ResourceList{}
		}
		dpPodObj.Spec.Containers[0].Resources.Requests[corev1.ResourceName(extendedResource)] = resource.MustParse("1")

		_, err := clients.Pods(namespaces.Test).Create(ctx, dpPodObj, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		waitForPodRunningOnNode(ctx, dpPodName, node)
	})
})
