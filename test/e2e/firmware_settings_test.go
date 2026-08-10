//go:build e2e

package e2e

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/cluster-api/util/patch"
)

// biosSettingName is a BIOS attribute supported by both Dell iDRAC and sushy-tools.
const biosSettingName = "ProcTurboMode"

var _ = Describe("Firmware settings", Label("firmware-settings"), func() {
	var (
		bmh           metal3api.BareMetalHost
		hfs           *metal3api.HostFirmwareSettings
		newValue      string
		initialState  metal3api.ProvisioningState
		cancelMonitor context.CancelFunc
		pingMu        sync.Mutex
		pingHistory   []byte
		screenshotDir string
		logStartTime  time.Time
	)

	BeforeEach(func() {
		bmhKey := types.NamespacedName{Namespace: bmc.Namespace, Name: bmc.Name}

		bmh = metal3api.BareMetalHost{}
		switch err := clusterProxy.GetClient().Get(ctx, bmhKey, &bmh); {
		case k8serrors.IsNotFound(err):
			By(fmt.Sprintf("Creating BMH %s", bmhKey))
			bmh, err = createBMH(ctx, bmhKey, clusterProxy)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for BMH to become available")
			WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
				Client: clusterProxy.GetClient(),
				Bmh:    bmh,
				State:  metal3api.StateAvailable,
			}, e2eConfig.GetIntervals("default", "wait-available")...)
		case err != nil:
			Expect(err).NotTo(HaveOccurred())
		}

		initialState = bmh.Status.Provisioning.State
		fmt.Printf("INFO: BMH %s/%s is in state %s\n", bmh.Namespace, bmh.Name, initialState)
		Expect(initialState).To(BeElementOf(metal3api.StateAvailable, metal3api.StateProvisioned),
			fmt.Sprintf("BMH must be available or provisioned, got %s", initialState))

		logClusterInfo(ctx, clusterProxy)

		if initialState == metal3api.StateProvisioned {
			Expect(WaitOCPReady(ctx, bmc.KubeconfigPath)).To(BeTrue(),
				fmt.Sprintf("OCP cluster on BMH %s/%s must be ready before test", bmh.Namespace, bmh.Name))
			fmt.Printf("INFO: OCP cluster on BMH %s/%s is up and running\n", bmh.Namespace, bmh.Name)

			By("Creating HostUpdatePolicy with firmwareSettings: onReboot")
			hup := &metal3api.HostUpdatePolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      bmh.Name,
					Namespace: bmh.Namespace,
				},
				Spec: metal3api.HostUpdatePolicySpec{
					FirmwareSettings: metal3api.HostUpdatePolicyOnReboot,
				},
			}
			err := clusterProxy.GetClient().Create(ctx, hup)
			if !k8serrors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}
		}

		By("Reading the HostFirmwareSettings resource")
		hfs = &metal3api.HostFirmwareSettings{}
		hfsKey := types.NamespacedName{Namespace: bmh.Namespace, Name: bmh.Name}
		Expect(clusterProxy.GetClient().Get(ctx, hfsKey, hfs)).To(Succeed())

		originalValue, ok := hfs.Status.Settings[biosSettingName]
		Expect(ok).To(BeTrue(), fmt.Sprintf("%s setting not found in HostFirmwareSettings", biosSettingName))
		newValue = "Enabled"
		if originalValue == "Enabled" {
			newValue = "Disabled"
		}
		Logf("Current %s = %q, changing to %q", biosSettingName, originalValue, newValue)
	})

	BeforeEach(func() {
		logStartTime = time.Now()
		pingMu.Lock()
		pingHistory = pingHistory[:0]
		pingMu.Unlock()
		suiteConfig, _ := GinkgoConfiguration()
		screenshotDir = filepath.Join(artifactFolder, fmt.Sprintf("%d", suiteConfig.RandomSeed))
		Expect(os.MkdirAll(filepath.Join(screenshotDir, "console"), 0755)).To(Succeed())
		absScreenshotDir, _ := filepath.Abs(screenshotDir)
		fmt.Printf("[monitor] screenshots folder: %s\n", absScreenshotDir)
		fmt.Printf("[monitor] bmc endpoint: %s\n", bmc.Address)
		fmt.Printf("[monitor] ping target: %s\n", bmc.IPAddress)
		var monCtx context.Context
		monCtx, cancelMonitor = context.WithCancel(ctx)
		bmhKey := types.NamespacedName{Namespace: bmh.Namespace, Name: bmh.Name}
		ironicURL, ironicUser, ironicPass := ironicCredentials(ctx, clusterProxy)
		// Use BMH provisioning ID (Ironic UUID) if available, fall back to namespace~name.
		ironicNodeID := bmh.Namespace + "~" + bmh.Name
		if bmh.Status.Provisioning.ID != "" {
			ironicNodeID = bmh.Status.Provisioning.ID
		}
		ironicNode := ironicURL + "/v1/nodes/" + ironicNodeID
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			var lastState string
			lastChange := time.Now()
			var ipaLastHeartbeat string
			var ipaLastSeen time.Time
			for {
				select {
				case <-monCtx.Done():
					return
				case <-ticker.C:
					ping := "0"
					if err := exec.CommandContext(monCtx, "ping", "-c", "1", "-W", "2", bmc.IPAddress).Run(); err == nil {
						ping = "1"
					}
					if monCtx.Err() != nil {
						return
					}
					pingMu.Lock()
					pingHistory = append(pingHistory, ping[0])
					pingMu.Unlock()
					rf := redfishStatus(monCtx, bmc.Address, bmc.User, bmc.Password)
					if !rf.Available {
						continue
					}

					bmhState := "?"
					var cur metal3api.BareMetalHost
					if err := clusterProxy.GetClient().Get(monCtx, bmhKey, &cur); err == nil {
						bmhState = string(cur.Status.Provisioning.State)
					}

					ni := getIronicNodeInfo(monCtx, ironicNode, ironicUser, ironicPass)
					// Track IPA activity: last_heartbeat only changes when IPA is sending heartbeats.
					// This detects IPA even if agent_url is cleared between our 5s polls.
					if ni.LastHeartbeat != "" && ni.LastHeartbeat != ipaLastHeartbeat {
						ipaLastHeartbeat = ni.LastHeartbeat
						ipaLastSeen = time.Now()
					}
					ironicState := ni.ProvisionState
					if !ipaLastSeen.IsZero() {
						age := int(time.Since(ipaLastSeen).Seconds())
						if ni.AgentURL != "" {
							ironicState += fmt.Sprintf("+ipa(%ds)", age)
						} else if age < 120 {
							ironicState += fmt.Sprintf("+ipa!(%ds)", age)
						}
					}

					vmedia := redfishVirtualMedia(monCtx, bmc.Address, bmc.User, bmc.Password)

					now := time.Now()
					dest := filepath.Join(screenshotDir, "console", fmt.Sprintf("%s.jpeg", now.Format("20060102-150405")))
					_ = idracConsoleScreenshot(monCtx, bmc.Address, bmc.User, bmc.Password, dest)

					state := rf.PowerState + "|" + rf.BootProgress + "|" + ping + "|" + bmhState + "|" + ironicState + "|" + vmedia
					if state != lastState {
						dur := now.Sub(lastChange).Truncate(time.Second)
						fmt.Printf("[monitor] %s (%3ds) | power=%-3s, boot=%s, bmh=%s, ironic=%s, ping=%s, vmedia=%s\n",
							now.Format("15:04:05"), int(dur.Seconds()), rf.PowerState, rf.BootProgress, bmhState, ironicState, ping, vmedia)
						lastState = state
						lastChange = now
					}
				}
			}
		}()
	})

	AfterEach(func() {
		By("Stopping monitors")
		cancelMonitor()

		By("Saving Ironic and BMO logs since test start")
		_ = os.MkdirAll(screenshotDir, 0755)
		ironicLog := filepath.Join(screenshotDir, "ironic.log")
		bmoLog := filepath.Join(screenshotDir, "bmo.log")
		fmt.Printf("[logs] ironic log: %s\n", ironicLog)
		fmt.Printf("[logs] bmo log: %s\n", bmoLog)
		if err := savePodLogs(ctx, clusterProxy, "openshift-machine-api", "baremetal.openshift.io/cluster-baremetal-operator=metal3-state", "metal3-ironic", logStartTime, ironicLog); err != nil {
			fmt.Printf("[logs] ironic save failed: %v\n", err)
		}
		if err := savePodLogs(ctx, clusterProxy, "openshift-machine-api", "baremetal.openshift.io/cluster-baremetal-operator=metal3-baremetal-operator", "metal3-baremetal-operator", logStartTime, bmoLog); err != nil {
			fmt.Printf("[logs] bmo save failed: %v\n", err)
		}

		rf := redfishStatus(ctx, bmc.Address, bmc.User, bmc.Password)
		pingMu.Lock()
		hist := string(pingHistory)
		pingMu.Unlock()
		fmt.Printf("[monitor] final | power=%s, boot=%s, ping=%s\n",
			rf.PowerState, rf.BootProgress,
			func() string {
				if err := exec.CommandContext(ctx, "ping", "-c", "1", "-W", "2", bmc.IPAddress).Run(); err == nil {
					return "1"
				}
				return "0"
			}())
		fmt.Printf("[monitor] ping history: %s\n", hist)
		fmt.Printf("[monitor] screenshots folder: %s\n", screenshotDir)
		absScreenshotDir, _ := filepath.Abs(screenshotDir)
		fmt.Printf("[monitor] to create video: ffmpeg -framerate 1 -f image2 -vcodec mjpeg -pattern_type glob -i '%s/console/*.jpeg' -c:v libx264 -pix_fmt yuv420p out.mp4\n", absScreenshotDir)

		if initialState == metal3api.StateProvisioned {
			By("Deleting HostUpdatePolicy")
			hup := &metal3api.HostUpdatePolicy{}
			if err := clusterProxy.GetClient().Get(ctx, types.NamespacedName{Namespace: bmh.Namespace, Name: bmh.Name}, hup); err == nil {
				_ = clusterProxy.GetClient().Delete(ctx, hup)
			}
		}
	})

	It("should toggle turbo BIOS setting", func() {
		By("Patching HFS spec.settings to change ProcTurboMode")
		helper, err := patch.NewHelper(hfs, clusterProxy.GetClient())
		Expect(err).NotTo(HaveOccurred())
		hfs.Spec.Settings = metal3api.DesiredSettingsMap{
			biosSettingName: intstr.FromString(newValue),
		}
		Expect(helper.Patch(ctx, hfs)).To(Succeed())

		if initialState == metal3api.StateProvisioned {
			By("Annotating BMH to trigger reboot for servicing")
			rebootValue := "{}"
			AnnotateBmh(ctx, clusterProxy.GetClient(), bmh, metal3api.RebootAnnotationPrefix, &rebootValue)
		}

		By(fmt.Sprintf("Waiting for HFS status.settings[%s] to become %q", biosSettingName, newValue))
		hfsKey := types.NamespacedName{Namespace: bmh.Namespace, Name: bmh.Name}
		Eventually(func(g Gomega) {
			updatedHfs := &metal3api.HostFirmwareSettings{}
			g.Expect(clusterProxy.GetClient().Get(ctx, hfsKey, updatedHfs)).To(Succeed())
			g.Expect(updatedHfs.Status.Settings[biosSettingName]).To(Equal(newValue))
		}, "25m", "5s").Should(Succeed())

		By(fmt.Sprintf("Waiting for BMH to return to %s", initialState))
		WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
			Client: clusterProxy.GetClient(),
			Bmh:    bmh,
			State:  initialState,
		}, "25m", "5s")

		if initialState == metal3api.StateProvisioned {
			By("Waiting for BMH operational status to return to OK after servicing")
			WaitForBmhInOperationalStatus(ctx, WaitForBmhInOperationalStatusInput{
				Client: clusterProxy.GetClient(),
				Bmh:    bmh,
				State:  metal3api.OperationalStatusOK,
			}, "25m", "5s")

			By("Waiting for OCP cluster to be available")
			Eventually(func() bool {
				return WaitOCPReady(ctx, bmc.KubeconfigPath)
			}, "30m", "15s").Should(BeTrue())
		}

	})
})

type redfishResult struct {
	PowerState   string
	BootProgress string
	Available    bool
}

// redfishVirtualMedia queries the iDRAC VirtualMedia CD slot and returns the
// filename of the currently inserted image, or "-" if nothing is mounted.
func redfishVirtualMedia(ctx context.Context, bmcAddress, user, password string) string {
	idx := strings.Index(bmcAddress, "https://")
	if idx < 0 {
		return "-"
	}
	host := strings.SplitN(bmcAddress[idx+len("https://"):], "/", 2)[0]
	endpoint := "https://" + host + "/redfish/v1/Managers/iDRAC.Embedded.1/VirtualMedia/CD"

	c := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // #nosec G402
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return "-"
	}
	req.SetBasicAuth(user, password)
	resp, err := c.Do(req)
	if err != nil {
		return "-"
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "-"
	}

	var vm struct {
		Inserted bool   `json:"Inserted"`
		Image    string `json:"Image"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&vm); err != nil || !vm.Inserted || vm.Image == "" {
		return "-"
	}
	// Return only the filename part of the URL to keep the monitor line short.
	parts := strings.Split(strings.TrimRight(vm.Image, "/"), "/")
	return parts[len(parts)-1]
}

// redfishStatus queries the Redfish Systems endpoint and returns
// power state and boot progress info.
func redfishStatus(ctx context.Context, bmcAddress, user, password string) redfishResult {
	idx := strings.Index(bmcAddress, "http")
	if idx < 0 {
		return redfishResult{}
	}
	redfishURL := bmcAddress[idx:]

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // #nosec G402
			},
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, redfishURL, http.NoBody)
	if err != nil {
		return redfishResult{}
	}
	req.SetBasicAuth(user, password)

	resp, err := client.Do(req)
	if err != nil {
		return redfishResult{}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return redfishResult{}
	}

	var system struct {
		PowerState   string `json:"PowerState"`
		BootProgress struct {
			LastState string `json:"LastState"`
		} `json:"BootProgress"`
	}
	if err := json.Unmarshal(body, &system); err != nil || system.PowerState == "" {
		return redfishResult{}
	}

	boot := system.BootProgress.LastState
	switch boot {
	case "OEM", "MemoryInitializationStarted",
		"SystemHardwareInitializationComplete",
		"PCIResourceConfigStarted":
		boot = "POST"
	}

	return redfishResult{
		PowerState:   system.PowerState,
		BootProgress: boot,
		Available:    true,
	}
}

// ironicCredentials reads the Ironic URL and credentials from the cluster.
// On OCP it discovers them from the metal3-state EndpointSlice and metal3-ironic-password
// secret in openshift-machine-api. Otherwise it falls back to e2e config variables.
func ironicCredentials(ctx context.Context, proxy framework.ClusterProxy) (url, user, pass string) {
	ns := "openshift-machine-api"

	// Try OCP: read credentials from secret.
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{Namespace: ns, Name: "metal3-ironic-password"}
	if err := proxy.GetClient().Get(ctx, secretKey, secret); err == nil {
		user = string(secret.Data["username"])
		pass = string(secret.Data["password"])

		// Discover Ironic IP from metal3-state EndpointSlice via clientset.
		slices, err := proxy.GetClientSet().DiscoveryV1().EndpointSlices(ns).List(ctx,
			metav1.ListOptions{LabelSelector: "kubernetes.io/service-name=metal3-state"})
		if err == nil {
			for _, slice := range slices.Items {
				for _, port := range slice.Ports {
					if port.Name != nil && *port.Name == "ironic-api" && port.Port != nil {
						for _, ep := range slice.Endpoints {
							if len(ep.Addresses) > 0 {
								url = fmt.Sprintf("https://%s",
									net.JoinHostPort(ep.Addresses[0], fmt.Sprintf("%d", *port.Port)))
								fmt.Printf("[monitor] ironic endpoint: %s\n", url)
								pods, _ := proxy.GetClientSet().CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
								for _, pod := range pods.Items {
									for _, c := range pod.Spec.Containers {
										if c.Name == "metal3-ironic" || c.Name == "ironic" {
											fmt.Printf("[monitor] ironic image: %s\n", c.Image)
										}
									}
								}
								return url, user, pass
							}
						}
					}
				}
			}
		}

		// EndpointSlice not found, build from e2eConfig.
		ip := e2eConfig.GetVariable("IRONIC_PROVISIONING_IP")
		port := e2eConfig.GetVariable("IRONIC_PROVISIONING_PORT")
		url = fmt.Sprintf("https://%s", net.JoinHostPort(ip, port))
		fmt.Printf("[monitor] ironic endpoint (e2eConfig): %s\n", url)
		return url, user, pass
	}

	// Fall back to e2e config variables.
	ip := e2eConfig.GetVariable("IRONIC_PROVISIONING_IP")
	port := e2eConfig.GetVariable("IRONIC_PROVISIONING_PORT")
	url = fmt.Sprintf("https://%s", net.JoinHostPort(ip, port))
	user = e2eConfig.GetVariable("IRONIC_USERNAME")
	pass = e2eConfig.GetVariable("IRONIC_PASSWORD")
	fmt.Printf("[monitor] ironic endpoint (fallback): %s\n", url)
	return url, user, pass
}

type ironicNodeInfo struct {
	ProvisionState string
	AgentURL       string
	LastHeartbeat  string
}

// getIronicNodeInfo queries the Ironic API for a node's provision_state, agent_url,
// and last_heartbeat.
func getIronicNodeInfo(ctx context.Context, nodeURL, user, pass string) ironicNodeInfo {
	c := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // #nosec G402
			},
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeURL, http.NoBody)
	if err != nil {
		return ironicNodeInfo{ProvisionState: "req:" + err.Error()}
	}
	req.SetBasicAuth(user, pass)

	resp, err := c.Do(req)
	if err != nil {
		return ironicNodeInfo{ProvisionState: "conn:" + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ironicNodeInfo{ProvisionState: fmt.Sprintf("http:%d", resp.StatusCode)}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ironicNodeInfo{ProvisionState: "read:" + err.Error()}
	}

	var node struct {
		ProvisionState string `json:"provision_state"`
		AgentURL       string `json:"agent_url"`
		LastHeartbeat  string `json:"last_heartbeat"`
	}
	if err := json.Unmarshal(body, &node); err != nil {
		return ironicNodeInfo{ProvisionState: "json:" + err.Error()}
	}

	return ironicNodeInfo{
		ProvisionState: node.ProvisionState,
		AgentURL:       node.AgentURL,
		LastHeartbeat:  node.LastHeartbeat,
	}
}

// WaitOCPReady checks once if the OCP cluster is available by reading the
// ClusterVersion "version" resource and checking its Available condition.
// Returns true immediately if kubeconfigPath is empty.
func WaitOCPReady(ctx context.Context, kubeconfigPath string) bool {
	if kubeconfigPath == "" {
		return true
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return false
	}
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		return false
	}

	cv := &unstructured.Unstructured{}
	cv.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "config.openshift.io",
		Version: "v1",
		Kind:    "ClusterVersion",
	})
	if err := c.Get(ctx, types.NamespacedName{Name: "version"}, cv); err != nil {
		return false
	}
	conditions, found, err := unstructured.NestedSlice(cv.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, cond := range conditions {
		m, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}
		if m["type"] == "Available" {
			return m["status"] == "True"
		}
	}
	return false
}

// idracConsoleScreenshot captures the current server console display via the Dell iDRAC
// Redfish OEM action and writes it as a PNG file to destPath.
func idracConsoleScreenshot(ctx context.Context, bmcAddress, user, password, destPath string) error {
	idx := strings.Index(bmcAddress, "https://")
	if idx < 0 {
		return fmt.Errorf("no https:// in bmc address")
	}
	host := strings.SplitN(bmcAddress[idx+len("https://"):], "/", 2)[0]
	endpoint := "https://" + host + "/redfish/v1/Managers/iDRAC.Embedded.1/Oem/Dell/DellLCService/Actions/DellLCService.ExportServerScreenShot"

	body := strings.NewReader(`{"FileType":"ServerScreenShot"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(user, password)

	c := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // #nosec G402
		},
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d", resp.StatusCode)
	}

	var result struct {
		ServerScreenShotFile string `json:"ServerScreenShotFile"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if result.ServerScreenShotFile == "" {
		return fmt.Errorf("empty ServerScreenShotFile in response")
	}

	png, err := base64.StdEncoding.DecodeString(result.ServerScreenShotFile)
	if err != nil {
		return err
	}
	return os.WriteFile(destPath, png, 0600)
}

// logClusterInfo prints INFO lines with hub ClusterVersion, BMO image, and Ironic image.
func logClusterInfo(ctx context.Context, proxy framework.ClusterProxy) {
	ns := "openshift-machine-api"

	cv := &unstructured.Unstructured{}
	cv.SetGroupVersionKind(schema.GroupVersionKind{Group: "config.openshift.io", Version: "v1", Kind: "ClusterVersion"})
	if err := proxy.GetClient().Get(ctx, types.NamespacedName{Name: "version"}, cv); err == nil {
		if history, found, _ := unstructured.NestedSlice(cv.Object, "status", "history"); found && len(history) > 0 {
			if entry, ok := history[0].(map[string]interface{}); ok {
				fmt.Printf("INFO: hub ClusterVersion=%s\n", entry["version"])
			}
		}
	}

	pods, err := proxy.GetClientSet().CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, pod := range pods.Items {
		for _, c := range pod.Spec.Containers {
			switch c.Name {
			case "metal3-baremetal-operator", "baremetal-operator":
				fmt.Printf("INFO: BMO image=%s\n", c.Image)
			case "metal3-ironic", "ironic":
				fmt.Printf("INFO: Ironic image=%s\n", c.Image)
			}
		}
	}
}

// savePodLogs fetches logs from the first pod matching labelSelector since the
// given timestamp and writes them to destPath.
func savePodLogs(ctx context.Context, proxy framework.ClusterProxy, ns, labelSelector, container string, since time.Time, destPath string) error {
	pods, err := proxy.GetClientSet().CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("no pods found for %s", labelSelector)
	}
	sinceTime := metav1.NewTime(since)
	req := proxy.GetClientSet().CoreV1().Pods(ns).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{
		Container: container,
		SinceTime: &sinceTime,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Errorf("stream logs: %w", err)
	}
	defer stream.Close()
	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()
	_, err = io.Copy(f, stream)
	return err
}

func createBMH(ctx context.Context, target types.NamespacedName,
	clusterProxy framework.ClusterProxy) (metal3api.BareMetalHost, error) {
	secretName := "bmc-credentials"

	_, _ = framework.CreateNamespaceAndWatchEvents(ctx, framework.CreateNamespaceAndWatchEventsInput{
		Creator:             clusterProxy.GetClient(),
		ClientSet:           clusterProxy.GetClientSet(),
		Name:                target.Namespace,
		LogFolder:           artifactFolder,
		IgnoreAlreadyExists: true,
	})

	CreateSecret(ctx, clusterProxy.GetClient(), target.Namespace, secretName, map[string]string{
		"username": bmc.User,
		"password": bmc.Password,
	})

	bmh := metal3api.BareMetalHost{
		ObjectMeta: metav1.ObjectMeta{
			Name:      target.Name,
			Namespace: target.Namespace,
		},
		Spec: metal3api.BareMetalHostSpec{
			Online: true,
			BMC: metal3api.BMCDetails{
				Address:                        bmc.Address,
				CredentialsName:                secretName,
				DisableCertificateVerification: bmc.DisableCertificateVerification,
			},
			BootMode:       metal3api.BootMode(e2eConfig.GetVariable("BOOT_MODE")),
			BootMACAddress: bmc.BootMacAddress,
		},
	}
	if err := clusterProxy.GetClient().Create(ctx, &bmh); err != nil {
		return metal3api.BareMetalHost{}, fmt.Errorf("failed to create BMH: %w", err)
	}

	return bmh, nil
}
