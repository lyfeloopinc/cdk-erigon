package stages

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/log/v3"
)

type DatastreamClientRunner struct {
	dsClient   DatastreamClient
	logPrefix  string
	stopRunner atomic.Bool
	isReading  atomic.Bool
}

func NewDatastreamClientRunner(dsClient DatastreamClient, logPrefix string) *DatastreamClientRunner {
	return &DatastreamClientRunner{
		dsClient:  dsClient,
		logPrefix: logPrefix,
	}
}

func (r *DatastreamClientRunner) StartRead() error {
	r.dsClient.RenewEntryChannel()
	if r.isReading.Load() {
		return fmt.Errorf("tried starting datastream client runner thread while another is running")
	}

	r.stopRunner.Store(false)

	go func() {
		routineId := rand.Intn(1000000)

		log.Info(fmt.Sprintf("[%s] Started downloading L2Blocks routine ID: %d", r.logPrefix, routineId))
		defer log.Info(fmt.Sprintf("[%s] Ended downloading L2Blocks routine ID: %d", r.logPrefix, routineId))

		r.isReading.Store(true)
		defer r.isReading.Store(false)

		for {
			if r.stopRunner.Load() {
				break
			}

			// this will download all blocks from datastream and push them in a channel
			// if no error, break, else continue trying to get them
			// Create bookmark
			if err := r.dsClient.EnsureConnected(); err != nil {
				log.Error(fmt.Sprintf("[%s] Error connecting to datastream", r.logPrefix), "error", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if err := r.dsClient.ReadAllEntriesToChannel(); err != nil {

				log.Error(fmt.Sprintf("[%s] Error downloading blocks from datastream", r.logPrefix), "error", err)

				time.Sleep(10 * time.Millisecond)
				continue
			}
			break
		}
	}()

	return nil
}

func (r *DatastreamClientRunner) StopRead() {
	r.stopRunner.Swap(true)
	r.dsClient.StopReadingToChannel()
	//wait for the read to stop, otherwise panic occurs
	for r.dsClient.GetStreamingAtomic().Load() {
		time.Sleep(10 * time.Microsecond)
	}
}
