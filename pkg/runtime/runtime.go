package runtime

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/config"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/environment"
	spice_http "github.com/spiceai/spiceai/pkg/http"
	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/tempdir"
	"github.com/spiceai/spiceai/pkg/version"
	"go.uber.org/zap"
)

type SpiceRuntime struct {
	config *config.SpiceConfiguration
	viper  *viper.Viper
}

var (
	runtime SpiceRuntime
	zaplog  *zap.Logger = loggers.ZapLogger()
)

func (r *SpiceRuntime) LoadConfig() error {
	if r.viper == nil {
		r.viper = viper.New()
	}

	var err error
	if r.config == nil {
		appDir := context.CurrentContext().AppDir()
		r.config, err = config.LoadRuntimeConfiguration(r.viper, appDir)
	}

	return err
}

func (r *SpiceRuntime) printStartupBanner(mode string) {
	fmt.Printf("- Runtime version: %s\n", version.Version())
	if mode != "" {
		fmt.Printf("- Mode: %s\n", mode)
	}
	fmt.Println(aurora.Green(fmt.Sprintf("- Listening on http://localhost:%d", runtime.config.HttpPort)))
	fmt.Println()
	fmt.Println("Use Ctrl-C to stop")
}

func SingleRun(manifestPath string) error {
	err := startRuntime()
	if err != nil {
		return err
	}

	aiEngineReady := make(chan bool, 1)
	err = aiengine.StartServer(aiEngineReady, true)
	if err != nil {
		return err
	}

	err = spice_http.NewServer(runtime.config.HttpPort).Start()
	if err != nil {
		return err
	}

	<-aiEngineReady

	runtime.printStartupBanner("Single training run")

	pod, err := initializePod(manifestPath)
	if err != nil {
		return err
	}

	err = environment.StartDataListeners(15)
	if err != nil {
		return err
	}

	err = aiengine.StartTraining(pod)
	if err != nil {
		return err
	}

	fmt.Println(aurora.Green("Exiting after single training run."))

	return nil
}

func Run() error {
	err := startRuntime()
	if err != nil {
		return err
	}

	aiEngineReady := make(chan bool)
	err = aiengine.StartServer(aiEngineReady, false)
	if err != nil {
		return err
	}

	err = spice_http.NewServer(runtime.config.HttpPort).Start()
	if err != nil {
		return err
	}

	<-aiEngineReady

	runtime.printStartupBanner("")

	err = runtime.scanForPods()
	if err != nil {
		log.Printf("error scanning for pods: %s", err.Error())
		return err
	}

	err = watchPods()
	if err != nil {
		zaplog.Sugar().Errorf("error watching for pods: %s", err.Error())
		return err
	}

	err = environment.StartDataListeners(15)
	if err != nil {
		return err
	}

	return nil
}

func (r *SpiceRuntime) scanForPods() error {
	_, err := os.Stat(context.CurrentContext().AppDir())
	if err != nil {
		// No .spice means no pods
		return nil
	}

	podsManifestDir := context.CurrentContext().PodsDir()
	_, err = os.Stat(podsManifestDir)
	if err != nil {
		// No spicepods means no pods
		return nil
	}

	d, err := os.Open(podsManifestDir)
	if err != nil {
		return err
	}

	files, err := d.Readdir(-1)
	d.Close()
	if err != nil {
		return err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		manifestPath := filepath.Join(podsManifestDir, f.Name())
		_, err = initializePod(manifestPath)
		if err != nil {
			log.Println(fmt.Errorf("error loading pod manifest %s: %w", manifestPath, err))
			continue
		}
	}

	return nil
}

func startRuntime() error {
	runtime = SpiceRuntime{}

	err := runtime.LoadConfig()
	if err != nil {
		return err
	}

	fmt.Println("Loading Spice runtime ...")

	return nil
}

func initializePod(manifestPath string) (*pods.Pod, error) {
	newPod, err := pods.LoadPodFromManifest(manifestPath)
	if err != nil {
		log.Println(fmt.Errorf("error loading pod manifest %s: %w", manifestPath, err))
		return nil, err
	}

	pods.CreateOrUpdatePod(newPod)
	err = aiengine.InitializePod(newPod)
	if err != nil {
		log.Println(fmt.Errorf("error initializing pod %s: %w", newPod.Name, err))
		return nil, err
	}

	for _, ds := range newPod.DataSources() {
		fmt.Printf("Loaded dataspace %s\n", aurora.BrightCyan(ds.Name()))
	}

	return newPod, nil
}

func Shutdown() {
	log.Println("Shutting down...")

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := aiengine.StopServer()
		if err != nil {
			zaplog.Sugar().Debug(err.Error())
			return
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		err := tempdir.RemoveAllCreatedTempDirectories()
		if err != nil {
			zaplog.Sugar().Debug(err.Error())
			return
		}
	}()

	wg.Wait()
}
