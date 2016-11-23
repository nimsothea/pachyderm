package cmds

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pachyderm/pachyderm/src/client/version"
	"github.com/pachyderm/pachyderm/src/server/pkg/deploy"
	"github.com/pachyderm/pachyderm/src/server/pkg/deploy/assets"
	_metrics "github.com/pachyderm/pachyderm/src/server/pkg/metrics"
	"github.com/spf13/cobra"
	"go.pedge.io/pkg/cobra"
	"go.pedge.io/pkg/exec"
)

func maybeKcCreate(dryRun bool, manifest *bytes.Buffer) error {
	if dryRun {
		_, err := os.Stdout.Write(manifest.Bytes())
		return err
	}
	return pkgexec.RunIO(
		pkgexec.IO{
			Stdin:  manifest,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		}, "kubectl", "create", "-f", "-")
}

// DeployCmd returns a cobra command for deploying a pachyderm cluster.
func DeployCmd(metrics bool) *cobra.Command {
	var shards int
	var hostPath string
	var dev bool
	var dryRun bool
	var rethinkdbCacheSize string
	var logLevel string
	var opts *assets.AssetOpts

	deployLocal := &cobra.Command{
		Use:   "local",
		Short: "Deploy a single-node Pachyderm cluster with local metadata storage.",
		Long:  "Deploy a single-node Pachyderm cluster with local metadata storage.",
		Run: pkgcobra.RunBoundedArgs(pkgcobra.Bounds{Min: 0, Max: 0}, func(args []string) (retErr error) {
			if metrics && !dev {
				finalMetrics := _metrics.ReportAndFlushUserAction("Deploy")
				defer func(start time.Time) { finalMetrics(start, retErr) }(time.Now())
			}
			manifest := &bytes.Buffer{}
			if dev {
				opts.Version = deploy.DevVersionTag
			}
			assets.WriteLocalAssets(manifest, opts, hostPath)
			return maybeKcCreate(dryRun, manifest)
		}),
	}
	deployLocal.Flags().StringVar(&hostPath, "host-path", "/var/pachyderm", "Location on the host machine where PFS metadata will be stored.")
	deployLocal.Flags().BoolVarP(&dev, "dev", "d", false, "Don't use a specific version of pachyderm/pachd.")

	deployGoogle := &cobra.Command{
		Use:   "google <GCS bucket> <GCE persistent disks> <Disk size (in GB)>",
		Short: "Deploy a Pachyderm cluster running on GCP.",
		Long: "Deploy a Pachyderm cluster running on GCP. Arguments are:\n" +
			"  <GCS bucket>: A GCS bucket where Pachyderm will store PFS data.\n" +
			"  <GCE persistent disks>: A comma-separated list of GCE persistent disks, one per rethink shard (see --shards).\n" +
			"  <Disk size>: Size of GCE persistent disks (assumed to all be the same).\n",
		Run: pkgcobra.RunBoundedArgs(pkgcobra.Bounds{Min: 3, Max: 3}, func(args []string) (retErr error) {
			if metrics && !dev {
				finalMetrics := _metrics.ReportAndFlushUserAction("Deploy")
				defer func(start time.Time) { finalMetrics(start, retErr) }(time.Now())
			}
			volumeNames := strings.Split(args[1], ",")
			volumeSize, err := strconv.Atoi(args[2])
			if err != nil {
				return fmt.Errorf("volume size needs to be an integer; instead got %v", args[2])
			}
			manifest := &bytes.Buffer{}
			assets.WriteGoogleAssets(manifest, opts, args[0], volumeNames, volumeSize)
			return maybeKcCreate(dryRun, manifest)
		}),
	}

	deployAmazon := &cobra.Command{
		Use:   "amazon <S3 bucket> <id> <secret> <token> <region> <EBS volume name> <volume size (in GB)>",
		Short: "Deploy a Pachyderm cluster running on AWS.",
		Long:  "Deploy a Pachyderm cluster running on AWS.",
		Run: pkgcobra.RunBoundedArgs(pkgcobra.Bounds{Min: 7, Max: 7}, func(args []string) (retErr error) {
			if metrics && !dev {
				finalMetrics := _metrics.ReportAndFlushUserAction("Deploy")
				defer func(start time.Time) { finalMetrics(start, retErr) }(time.Now())
			}
			volumeNames := strings.Split(args[5], ",")
			volumeSize, err := strconv.Atoi(args[6])
			if err != nil {
				return fmt.Errorf("volume size needs to be an integer; instead got %v", args[6])
			}
			manifest := &bytes.Buffer{}
			assets.WriteAmazonAssets(manifest, opts, args[0], args[1], args[2], args[3],
				args[4], volumeNames, volumeSize)
			return maybeKcCreate(dryRun, manifest)
		}),
	}

	deployMicrosoft := &cobra.Command{
		Use:   "microsoft <container> <storage account name> <storage account key> <volume uri> <volume size in GB>",
		Short: "Deploy a Pachyderm cluster running on Microsoft Azure.",
		Long:  "Deploy a Pachyderm cluster running on Microsoft Azure.",
		Run: pkgcobra.RunBoundedArgs(pkgcobra.Bounds{Min: 5, Max: 5}, func(args []string) (retErr error) {
			if metrics && !dev {
				finalMetrics := _metrics.ReportAndFlushUserAction("Deploy")
				defer func(start time.Time) { finalMetrics(start, retErr) }(time.Now())
			}
			if _, err := base64.StdEncoding.DecodeString(args[2]); err != nil {
				return fmt.Errorf("storage-account-key needs to be base64 encoded; instead got '%v'", args[2])
			}
			volumeURIs := strings.Split(args[3], ",")
			for i, uri := range volumeURIs {
				tempURI, err := url.ParseRequestURI(uri)
				if err != nil {
					return fmt.Errorf("All volume-uris needs to be a well-formed URI; instead got '%v'", uri)
				}
				volumeURIs[i] = tempURI.String()
			}
			volumeSize, err := strconv.Atoi(args[4])
			if err != nil {
				return fmt.Errorf("volume size needs to be an integer; instead got %v", args[4])
			}
			manifest := &bytes.Buffer{}
			assets.WriteMicrosoftAssets(manifest, opts, args[0], args[1], args[2], volumeURIs, volumeSize)
			return maybeKcCreate(dryRun, manifest)
		}),
	}

	cmd := &cobra.Command{
		Use:   "deploy amazon|google|microsoft|basic",
		Short: "Deploy a Pachyderm cluster.",
		Long:  "Deploy a Pachyderm cluster.",
		PersistentPreRun: func(*cobra.Command, []string) {
			// fmt.Printf("shards: %d\n", shards)
			opts = &assets.AssetOpts{
				Shards:             uint64(shards),
				RethinkdbCacheSize: rethinkdbCacheSize,
				Version:            version.PrettyPrintVersion(version.Version),
				LogLevel:           logLevel,
				Metrics:            metrics,
			}
		},
	}
	cmd.PersistentFlags().IntVarP(&shards, "shards", "s", 1, "The static number of RethinkDB shards (for pfs metadata storage).")
	cmd.PersistentFlags().BoolVarP(&dryRun, "dry-run", "", false, "Don't actually deploy pachyderm to Kubernetes, instead just print the manifest.")
	cmd.PersistentFlags().StringVar(&rethinkdbCacheSize, "rethinkdb-cache-size", "768M", "Size of in-memory cache to use for Pachyderm's RethinkDB instance, "+
		"e.g. \"2G\". Default is \"768M\". Size is specified in bytes, with allowed SI suffixes (M, K, G, Mi, Ki, Gi, etc)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "The level of log messages to print options are, from least to most verbose: \"error\", \"info\", \"debug\".")
	cmd.AddCommand(deployLocal)
	cmd.AddCommand(deployAmazon)
	cmd.AddCommand(deployGoogle)
	cmd.AddCommand(deployMicrosoft)
	return cmd
}
