//go:build e2e

package e2e

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/util/patch"
)

// biosSettingName is a BIOS attribute supported by both Dell iDRAC and sushy-tools.
const biosSettingName = "ProcTurboMode"

var _ = Describe("Firmware settings", Label("firmware-settings"), func() {
	var (
		bmh           metal3api.BareMetalHost
		hfs           *metal3api.HostFirmwareSettings
		newValue      string
		cancelMonitor context.CancelFunc
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

		state := bmh.Status.Provisioning.State
		Logf("BMH %s/%s is in state %s", bmh.Namespace, bmh.Name, state)
		Expect(state).To(BeElementOf(metal3api.StateAvailable, metal3api.StateProvisioned),
			fmt.Sprintf("BMH must be available or provisioned, got %s", state))

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
		By("Starting ping and redfish monitors on " + bmc.IPAddress)
		var monCtx context.Context
		monCtx, cancelMonitor = context.WithCancel(ctx)
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			var lastState string
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
					power := redfishPowerState(monCtx, bmc.Address, bmc.User, bmc.Password)
					if power == "Unavailable" || power == "ConnError" {
						continue
					}

					state := power + "|" + ping
					if state != lastState {
						fmt.Printf("[monitor] %s | power=%s | ping=%s\n",
							time.Now().Format("15:04:05"), power, ping)
						lastState = state
					}
				}
			}
		}()
	})

	AfterEach(func() {
		By("Stopping monitors")
		cancelMonitor()
	})

	It("should toggle turbo BIOS setting", func() {
		By("Patching HFS spec.settings to change ProcTurboMode")
		helper, err := patch.NewHelper(hfs, clusterProxy.GetClient())
		Expect(err).NotTo(HaveOccurred())
		hfs.Spec.Settings = metal3api.DesiredSettingsMap{
			biosSettingName: intstr.FromString(newValue),
		}
		Expect(helper.Patch(ctx, hfs)).To(Succeed())

		By(fmt.Sprintf("Waiting for HFS status.settings[%s] to become %q", biosSettingName, newValue))
		hfsKey := types.NamespacedName{Namespace: bmh.Namespace, Name: bmh.Name}
		Eventually(func(g Gomega) {
			updatedHfs := &metal3api.HostFirmwareSettings{}
			g.Expect(clusterProxy.GetClient().Get(ctx, hfsKey, updatedHfs)).To(Succeed())
			g.Expect(updatedHfs.Status.Settings[biosSettingName]).To(Equal(newValue))
		}, "25m", "5s").Should(Succeed())

		By("Waiting for BMH to return to available")
		WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
			Client: clusterProxy.GetClient(),
			Bmh:    bmh,
			State:  metal3api.StateAvailable,
		}, "25m", "5s")
	})
})

// redfishPowerState queries the Redfish Systems endpoint and returns
// the PowerState value (On, Off, etc.) or "Unavailable" when the BMC
// cannot provide system data (e.g. during a reboot).
func redfishPowerState(ctx context.Context, bmcAddress, user, password string) string {
	idx := strings.Index(bmcAddress, "http")
	if idx < 0 {
		return "BadAddress"
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
		return "ReqError"
	}
	req.SetBasicAuth(user, password)

	resp, err := client.Do(req)
	if err != nil {
		return "ConnError"
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "ReadError"
	}

	var system struct {
		PowerState string `json:"PowerState"`
	}
	if err := json.Unmarshal(body, &system); err != nil {
		return "ParseError"
	}
	if system.PowerState == "" {
		return "Unavailable"
	}

	return system.PowerState
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
