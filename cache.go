package cache

import (
	"errors"
	"fmt"
	"strconv"
	"time"
	env "github.com/SoleaEnergy/fission-env-resolver"
	"github.com/couchbase/gocb/v2"
	"github.com/golang-module/carbon"
)

const BulkLoadBatchSize = 500

func handleConnectionError(err error) {
	if err != nil {
		fmt.Println("Unable to connect to couchbase!! Exiting")
		fmt.Println(err)
	}
}

func Connect() (*gocb.Cluster, *gocb.Bucket) {
	cbHost := env.Resolve("couchbase_host")
	cbUser := env.Resolve("couchbase_username")
	cbPassword := env.Resolve("couchbase_pw")
	cbBucketName := env.Resolve("couchbase_bucketname")

	clusterOpts := gocb.ClusterOptions{
		Username: cbUser,
		Password: cbPassword,
	}
	cluster, err := gocb.Connect(cbHost, clusterOpts)
	if err != nil {
		handleConnectionError(err)
		return nil, nil
	}

	// Test our bucket connection before declaring success
	bucket := cluster.Bucket(cbBucketName)

	// We wait until the bucket is definitely connected and setup.
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		handleConnectionError(err)
		return nil, nil
	}

	return cluster, bucket
}

//Test CB connection before starting cache loading process
func TestConnect() (msg string, err error) {
	c, b := Connect()
	if c == nil && b == nil {
		return "failed", errors.New("couchbase failed to connect")
	}
	b.Name()
	c.Close(nil)
	return "Success", nil
}

type JobStatus struct {
	JobId               string   `json:"job_id"`
	SubId               string   `json:"sub_id"`
	ParamStartDate      string   `json:"param_start_date"`
	ParamEndDate        string   `json:"param_end_date"`
	ResultStatus        string   `json:"result_status"`
	NumRecordsProcessed string   `json:"results_processed"`
	JobDuration         string   `json:"duration"`
	Timestamp           string   `json:"timestamp"`
	ErrorCount          string   `json:"error_count"`
	ErrorMessages       []string `json:"errors"`
	JobType             string   `json:"job_type"`
	ExecutionType       string   `json:"execution_type"`
}

func getStatusMsg(status JobStatus) string {
	eCount, err := strconv.Atoi(status.ErrorCount)
	if err != nil {
		fmt.Println(err)
	}
	sCount, err := strconv.Atoi(status.NumRecordsProcessed)
	if err != nil {
		fmt.Println(err)
	}
	var msg string

	if eCount > 0 && sCount > 0 {
		msg = "Partial Success"
	} else if eCount > 0 && sCount == 0 {
		msg = "Failed"
	} else if eCount == 0 && sCount > 0 {
		msg = "Success"
	} else {
		msg = "Success"
	}
	return msg
}

func WriteStatus(status JobStatus) {
	c, cb := Connect()
	scope := cb.Scope("system")
	coll := scope.Collection("cachelogs")
	defer c.Close(nil)
	status.Timestamp = carbon.Now().ToDateTimeString()
	statusKey := status.JobId + status.SubId
	status.ResultStatus = getStatusMsg(status)
	coll.Upsert(statusKey, status, nil)
}

func FetchStatus(jobType string) []JobStatus {
	c, _ := Connect()
	defer c.Close(nil)
	selectFrom := "SELECT cl.* from `beyond`.`system`.`cachelogs` cl "
	where := " where job_type = "
	orderByLimit:= " ORDER BY timestamp desc LIMIT 50 "

	query := selectFrom + where + "'" + jobType + "' " + orderByLimit
	results, err := c.Query(query, &gocb.QueryOptions{})
	if err != nil {
		panic(err)
	}

	var status []JobStatus
	for results.Next() {
		var s JobStatus
		err := results.Row(&s)
		if err != nil {
			panic(err)
		}
		fmt.Println(status)
		status = append(status, s)
	}
	return status
}
