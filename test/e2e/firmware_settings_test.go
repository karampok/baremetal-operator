//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

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
		bmh      metal3api.BareMetalHost
		hfs      *metal3api.HostFirmwareSettings
		newValue string
		pingCmd  *exec.Cmd
		pingFile *os.File
	)

	BeforeEach(func() {
		bmhKey := types.NamespacedName{Namespace: bmc.Namespace, Name: bmc.Name}

		bmh = metal3api.BareMetalHost{}
		switch err := clusterProxy.GetClient().Get(ctx, bmhKey, &bmh); {
		case k8serrors.IsNotFound(err):
			By(fmt.Sprintf("Creating BMH %s", bmhKey))
			bmh, err = createBMH(ctx, bmhKey, clusterProxy)
			Expect(err).NotTo(HaveOccurred())
		case err != nil:
			Expect(err).NotTo(HaveOccurred())
		}

		By(fmt.Sprintf("Waiting for BMH %s/%s to become available", bmh.Namespace, bmh.Name))
		WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
			Client: clusterProxy.GetClient(),
			Bmh:    bmh,
			State:  metal3api.StateAvailable,
		}, e2eConfig.GetIntervals("default", "wait-available")...)

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
		By("Starting ping monitor on " + bmc.IPAddress)
		pingLogPath := filepath.Join(artifactFolder, "ping-monitor.log")
		var err error
		pingFile, err = os.OpenFile(pingLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		Expect(err).NotTo(HaveOccurred())

		absPath, _ := filepath.Abs(pingLogPath)
		Logf("Ping log: %s", absPath)

		pingCmd = exec.Command("ping", "-D", "-O", "-i", "1", bmc.IPAddress)
		pingCmd.Stdout = pingFile
		pingCmd.Stderr = pingFile
		Expect(pingCmd.Start()).To(Succeed())
	})

	AfterEach(func() {
		By("Stopping ping monitor")
		_ = pingCmd.Process.Signal(syscall.SIGINT)
		_ = pingCmd.Wait()
		pingFile.Close()

		By("Printing ping summary")
		data, err := os.ReadFile(pingFile.Name())
		Expect(err).NotTo(HaveOccurred())

		var visual strings.Builder
		for line := range strings.SplitSeq(string(data), "\n") {
			switch {
			case strings.Contains(line, "bytes from"):
				visual.WriteByte('1')
			case strings.Contains(line, "no answer"):
				visual.WriteByte('0')
			}
		}

		Logf("\n--- Ping Monitor (%s) ---", bmc.IPAddress)
		Logf("  %s", visual.String())
		Logf("  Log: %s", pingFile.Name())
		Logf("-----------------------------------")
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
		}, e2eConfig.GetIntervals("default", "wait-available")...).Should(Succeed())

		By("Waiting for BMH to return to available")
		WaitForBmhInProvisioningState(ctx, WaitForBmhInProvisioningStateInput{
			Client: clusterProxy.GetClient(),
			Bmh:    bmh,
			State:  metal3api.StateAvailable,
		}, e2eConfig.GetIntervals("default", "wait-available")...)
	})
})

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
