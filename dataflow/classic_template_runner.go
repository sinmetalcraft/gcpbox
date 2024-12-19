package dataflow

import (
	"context"
	"fmt"

	"strings"
	"time"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"github.com/sinmetalcraft/gcpbox/internal/times"
)

type ClassicTemplateRunner struct {
	cli         *dataflow.TemplatesClient
	jobCli      *dataflow.JobsV1Beta3Client
	timeService *times.TimeService
}

func NewClassicTemplateRunner(ctx context.Context, cli *dataflow.TemplatesClient, jobCli *dataflow.JobsV1Beta3Client) (*ClassicTemplateRunner, error) {
	timeService, err := times.NewService(ctx)
	if err != nil {
		return nil, err
	}
	return &ClassicTemplateRunner{
		cli:         cli,
		jobCli:      jobCli,
		timeService: timeService,
	}, nil
}

type ClassicLaunchTemplateJobRequest struct {
	ProjectID       string
	Location        string
	JobName         string
	TemplateGCSPath string
	Parameters      map[string]string
}

type ClassicLaunchTemplateRuntimeEnvironment struct {
	// The initial number of Google Compute Engine instnaces for the job.
	NumWorkers int32 `protobuf:"varint,11,opt,name=num_workers,json=numWorkers,proto3" json:"num_workers,omitempty"`
	// The maximum number of Google Compute Engine instances to be made
	// available to your pipeline during execution, from 1 to 1000.
	MaxWorkers int32 `protobuf:"varint,1,opt,name=max_workers,json=maxWorkers,proto3" json:"max_workers,omitempty"`
	// The Compute Engine [availability
	// zone](https://cloud.google.com/compute/docs/regions-zones/regions-zones)
	// for launching worker instances to run your pipeline.
	// In the future, worker_zone will take precedence.
	Zone string `protobuf:"bytes,2,opt,name=zone,proto3" json:"zone,omitempty"`
	// The email address of the service account to run the job as.
	ServiceAccountEmail string `protobuf:"bytes,3,opt,name=service_account_email,json=serviceAccountEmail,proto3" json:"service_account_email,omitempty"`
	// The Cloud Storage path to use for temporary files.
	// Must be a valid Cloud Storage URL, beginning with `gs://`.
	TempLocation string `protobuf:"bytes,4,opt,name=temp_location,json=tempLocation,proto3" json:"temp_location,omitempty"`
	// Whether to bypass the safety checks for the job's temporary directory.
	// Use with caution.
	BypassTempDirValidation bool `protobuf:"varint,5,opt,name=bypass_temp_dir_validation,json=bypassTempDirValidation,proto3" json:"bypass_temp_dir_validation,omitempty"`
	// The machine type to use for the job. Defaults to the value from the
	// template if not specified.
	MachineType string `protobuf:"bytes,6,opt,name=machine_type,json=machineType,proto3" json:"machine_type,omitempty"`
	// Additional experiment flags for the job, specified with the
	// `--experiments` option.
	AdditionalExperiments []string `protobuf:"bytes,7,rep,name=additional_experiments,json=additionalExperiments,proto3" json:"additional_experiments,omitempty"`
	// Network to which VMs will be assigned.  If empty or unspecified,
	// the service will use the network "default".
	Network string `protobuf:"bytes,8,opt,name=network,proto3" json:"network,omitempty"`
	// Subnetwork to which VMs will be assigned, if desired. You can specify a
	// subnetwork using either a complete URL or an abbreviated path. Expected to
	// be of the form
	// "https://www.googleapis.com/compute/v1/projects/HOST_PROJECT_ID/regions/REGION/subnetworks/SUBNETWORK"
	// or "regions/REGION/subnetworks/SUBNETWORK". If the subnetwork is located in
	// a Shared VPC network, you must use the complete URL.
	Subnetwork string `protobuf:"bytes,9,opt,name=subnetwork,proto3" json:"subnetwork,omitempty"`
	// Additional user labels to be specified for the job.
	// Keys and values should follow the restrictions specified in the [labeling
	// restrictions](https://cloud.google.com/compute/docs/labeling-resources#restrictions)
	// page.
	// An object containing a list of "key": value pairs.
	// Example: { "name": "wrench", "mass": "1kg", "count": "3" }.
	AdditionalUserLabels map[string]string `protobuf:"bytes,10,rep,name=additional_user_labels,json=additionalUserLabels,proto3" json:"additional_user_labels,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// Name for the Cloud KMS key for the job.
	// Key format is:
	// projects/<project>/locations/<location>/keyRings/<keyring>/cryptoKeys/<key>
	KmsKeyName string `protobuf:"bytes,12,opt,name=kms_key_name,json=kmsKeyName,proto3" json:"kms_key_name,omitempty"`
	// The Compute Engine region
	// (https://cloud.google.com/compute/docs/regions-zones/regions-zones) in
	// which worker processing should occur, e.g. "us-west1". Mutually exclusive
	// with worker_zone. If neither worker_region nor worker_zone is specified,
	// default to the control plane's region.
	WorkerRegion string `protobuf:"bytes,15,opt,name=worker_region,json=workerRegion,proto3" json:"worker_region,omitempty"`
	// The Compute Engine zone
	// (https://cloud.google.com/compute/docs/regions-zones/regions-zones) in
	// which worker processing should occur, e.g. "us-west1-a". Mutually exclusive
	// with worker_region. If neither worker_region nor worker_zone is specified,
	// a zone in the control plane's region is chosen based on available capacity.
	// If both `worker_zone` and `zone` are set, `worker_zone` takes precedence.
	WorkerZone string `protobuf:"bytes,16,opt,name=worker_zone,json=workerZone,proto3" json:"worker_zone,omitempty"`
	// Whether to enable Streaming Engine for the job.
	EnableStreamingEngine bool `protobuf:"varint,17,opt,name=enable_streaming_engine,json=enableStreamingEngine,proto3" json:"enable_streaming_engine,omitempty"`
}

// LaunchTemplateJob is Dataflow Jobを投入する
func (r *ClassicTemplateRunner) LaunchTemplateJob(ctx context.Context, req *ClassicLaunchTemplateJobRequest, runtime *ClassicLaunchTemplateRuntimeEnvironment) (*dataflowpb.LaunchTemplateResponse, error) {
	resp, err := r.cli.LaunchTemplate(ctx,
		&dataflowpb.LaunchTemplateRequest{
			ProjectId:    req.ProjectID,
			ValidateOnly: false,
			Template: &dataflowpb.LaunchTemplateRequest_GcsPath{
				GcsPath: req.TemplateGCSPath,
			},
			LaunchParameters: &dataflowpb.LaunchTemplateParameters{
				JobName:    req.JobName,
				Parameters: req.Parameters,
				Environment: &dataflowpb.RuntimeEnvironment{
					NumWorkers:              runtime.NumWorkers,
					MaxWorkers:              runtime.MaxWorkers,
					Zone:                    runtime.Zone,
					ServiceAccountEmail:     runtime.ServiceAccountEmail,
					TempLocation:            runtime.TempLocation,
					BypassTempDirValidation: runtime.BypassTempDirValidation,
					MachineType:             runtime.MachineType,
					AdditionalExperiments:   runtime.AdditionalExperiments,
					Network:                 runtime.Network,
					Subnetwork:              runtime.Subnetwork,
					AdditionalUserLabels:    runtime.AdditionalUserLabels,
					KmsKeyName:              runtime.KmsKeyName,
					IpConfiguration:         0,
					WorkerRegion:            runtime.WorkerRegion,
					WorkerZone:              runtime.WorkerZone,
					EnableStreamingEngine:   runtime.EnableStreamingEngine,
				},
				Update:               false,
				TransformNameMapping: nil,
			},
			Location: req.Location,
		})
	if err != nil {
		return nil, fmt.Errorf("failed ClassicTemplateRunner.LaunchTemplateJob : %w", err)
	}
	return resp, nil
}

type SpannerToAvroOnGCSJobRequest struct {
	// ProjectID is Dataflow Jobを実行するProjectID
	ProjectID string

	// Location is Dataflow Jobを実行するLocation
	Location string

	// JobName is Dataflow JobのJobName。
	// 指定しない場合は gcpbox-spanner-export-to-avro-on-gcs-YYYYMMDD-HHMMSS になります。
	JobName string

	// OutputDir is Avro ファイルをエクスポートする Cloud Storage パス。
	// エクスポート ジョブによって、このパスの下にディレクトリが新規作成されます。
	// ここに、エクスポートされたファイルが格納されます（例: gs://your-bucket/your-path）。
	OutputDir string

	// OutputDirAddCurrentDate is OutputDirにJSTでの現在日時を/yyyy/mm/ddで追加します
	OutputDirAddCurrentDateJST bool

	// AvroTempDirectory is 一時的な Avro ファイルが書き込まれる Cloud Storage パス
	//
	// Jobが完了した場合は最後に削除される
	// 宛先のbucketがversioningをenableにしていると、論理削除になって留まるので注意。
	// Lifecycleを指定して一定時間後に削除されるbucketを指定した方がよいです。
	// OutputDirにobjectがmoveされるので、同じLocationのbucketを指定するのがよいですが、対象のbucketがDual RegionやMulti Regionの場合はその中に含まれるLocationのSingle Region Bucketが無難です。
	AvroTempDirectory string

	// SnapshotTime is SpannerからExportを行うTimestamp Boundsの値
	// UTCで指定する
	//
	// 初期状態では現在時刻からマイナス1hの間で指定できる。
	// Databaseのversion_retention_periodを伸ばしている場合は、その間で指定できる。
	// https://cloud.google.com/spanner/docs/timestamp-bounds
	SnapshotTime time.Time

	// SnapshotTimeJSTDayChangeTime is 実行時刻の0時にSnapshotTimeを指定するようになる
	// SnapshotTimeと同時に指定した場合はSnapshotTimeが優先される
	SnapshotTimeJSTDayChangeTime bool

	// SpannerProjectID is ExportするSpannerがあるProjectID
	SpannerProjectID string

	// SpannerInstanceID is ExportするSpannerのInstanceID
	SpannerInstanceID string

	// SpannerDatabaseID is ExportするSpannerのDatabaseID
	SpannerDatabaseID string

	// ShouldExportTimestampAsLogicalType is true の場合、タイムスタンプは、timestamp-micros 論理型の long 型としてエクスポートされます。
	// デフォルトでは、このパラメータは false に設定され、タイムスタンプはナノ秒単位の精度で ISO-8601 文字列としてエクスポートされます。
	ShouldExportTimestampAsLogicalType bool

	// TableNames is エクスポートする Spanner データベースのサブセットを指定するテーブルのカンマ区切りリスト。
	// このパラメータを設定する場合は、すべての関連テーブル（親テーブルと外部キーで参照されるテーブル）を含めるか、shouldExportRelatedTables パラメータを true に設定する必要があります。
	// テーブルが名前付きスキーマに含まれている場合は、完全修飾名を使用してください。
	// たとえば、sch1.foo を使用します。ここで、sch1 はスキーマ名、foo はテーブル名です。デフォルトは空です。
	TableNames []string

	// ShouldExportRelatedTables is 関連するテーブルを含めるかどうか。
	// このパラメータは、tableNames パラメータと組み合わせて使用します。デフォルトは false です。
	ShouldExportRelatedTables bool

	// Spanner 呼び出しのリクエストの優先度。
	// 有効な値は HIGH、MEDIUM、LOW です。デフォルト値は MEDIUM です。
	SpannerPriority string

	// DataBoostEnabled is true に設定すると、Spanner Data Boost のコンピューティング リソースを使用してジョブを実行するときに、Spanner OLTP ワークフローへの影響をほぼゼロにすることができます。
	// true に設定する場合は、spanner.databases.useDataBoost IAM 権限も必要です。
	// 詳細については、Data Boost の概要（https://cloud.google.com/spanner/docs/databoost/databoost-overview）をご覧ください。デフォルトは false です。
	DataBoostEnabled bool

	// TemplateGCSPath is Dataflow TemplateのGCS Path
	// 指定してない場合 gs://dataflow-templates-{LOCATION}/latest/Cloud_Spanner_to_GCS_Avro が使われます。
	// latest以外のversionを使いたい場合に指定してください
	TemplateGCSPath string
}

// LaunchSpannerToAvroOnGCSJob is Spanner Export to Avro on GCSのDataflow Jobを投入する
func (r *ClassicTemplateRunner) LaunchSpannerToAvroOnGCSJob(ctx context.Context, req *SpannerToAvroOnGCSJobRequest, runtime *ClassicLaunchTemplateRuntimeEnvironment) (*dataflowpb.LaunchTemplateResponse, error) {
	jstNow := time.Now().In(r.timeService.JST)
	jstDayChangeTime := r.timeService.JSTDayChangeTime(jstNow)

	jobName := req.JobName
	if jobName == "" {
		jobName = fmt.Sprintf("gcpbox-spanner-export-to-avro-on-gcs-%s", jstNow.Format("20060102-150405"))
	}
	parameters := make(map[string]string)
	if req.OutputDir != "" && req.OutputDirAddCurrentDateJST {
		parameters["outputDir"] = fmt.Sprintf("%s/%s", req.OutputDir, jstDayChangeTime.Format("2006/01/02"))
	} else if req.OutputDir != "" {
		parameters["outputDir"] = req.OutputDir
	}

	if req.AvroTempDirectory != "" {
		parameters["avroTempDirectory"] = req.AvroTempDirectory
	}
	if req.SnapshotTimeJSTDayChangeTime {
		parameters["snapshotTime"] = jstDayChangeTime.In(time.UTC).Format("2006-01-02T15:04:05Z")
	}
	if !req.SnapshotTime.IsZero() {
		parameters["snapshotTime"] = req.SnapshotTime.Format("2006-01-02T15:04:05Z")
	}
	if req.SpannerProjectID != "" {
		parameters["spannerProjectId"] = req.SpannerProjectID
	}
	parameters["instanceId"] = req.SpannerInstanceID
	parameters["databaseId"] = req.SpannerDatabaseID
	if req.ShouldExportTimestampAsLogicalType {
		parameters["shouldExportTimestampAsLogicalType"] = "true"
	}
	if len(req.TableNames) > 0 {
		parameters["tableNames"] = strings.Join(req.TableNames, ",")
	}
	if req.ShouldExportRelatedTables {
		parameters["shouldExportRelatedTables"] = "true"
	}
	if req.SpannerPriority != "" {
		parameters["spannerPriority"] = req.SpannerPriority
	}
	if req.DataBoostEnabled {
		parameters["dataBoostEnabled"] = "true"
	}

	resp, err := r.LaunchTemplateJob(ctx,
		&ClassicLaunchTemplateJobRequest{
			ProjectID:       req.ProjectID,
			Location:        req.Location,
			JobName:         jobName,
			TemplateGCSPath: fmt.Sprintf("gs://dataflow-templates-%s/latest/Cloud_Spanner_to_GCS_Avro", req.Location),
			Parameters:      parameters,
		},
		runtime)
	if err != nil {
		return nil, fmt.Errorf("failed ClassicTemplateRunner.LaunchSpannerToAvroOnGCSJob : %w", err)
	}
	return resp, nil
}

func (r *ClassicTemplateRunner) GetJob(ctx context.Context, projectID string, location string, jobID string) (*dataflowpb.Job, error) {
	job, err := r.jobCli.GetJob(ctx, &dataflowpb.GetJobRequest{
		ProjectId: projectID,
		JobId:     jobID,
		View:      0,
		Location:  location,
	})
	if err != nil {
		return nil, fmt.Errorf("failed ClassicTemplateRunner.GetJob: %w", err)
	}
	return job, nil
}

// IsFinishJob is Jobが終了しているかを返す
// 終了しているだけなので、完遂したかは分からない
func (r *ClassicTemplateRunner) IsFinishJob(ctx context.Context, projectID string, location string, jobID string) (bool, error) {
	job, err := r.GetJob(ctx, projectID, location, jobID)
	if err != nil {
		return false, fmt.Errorf("failed ClassicTemplateRunner.IsDoneJob: %w", err)
	}
	switch job.GetCurrentState() {
	case dataflowpb.JobState_JOB_STATE_DONE, dataflowpb.JobState_JOB_STATE_FAILED, dataflowpb.JobState_JOB_STATE_CANCELLED:
		return true, nil
	}
	return false, nil
}
