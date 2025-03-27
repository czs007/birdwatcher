package states

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/storage"
)

type DownloadPrimaryKeyParams struct {
	framework.ParamBase `use:"load-pk" desc:"download to check data"`
	CollectionID        int64  `name:"collection" default:"0"`
	SegmentID           int64  `name:"segment" default:"0"`
	MinioAddress        string `name:"minioAddr"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket exist check due to permission issue"`
	IncludeUnhealthy    bool   `name:"includeUnhealthy" default:"false" desc:"also check dropped segments"`
}

type Entry struct {
	PK int64
	TS int64
}

func (s *InstanceState) DownloadBinlogCommand(ctx context.Context, p *DownloadPrimaryKeyParams) error {
	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		return err
	}
	fmt.Println("=== Checking collection schema ===")
	pkField, ok := collection.GetPKField()
	if !ok {
		return errors.New("pk field not found")
	}
	fmt.Printf("PK Field [%d] %s\n", pkField.FieldID, pkField.Name)

	fields := make(map[int64]models.FieldSchema) // make([]models.FieldSchema, 0, len(p.Fields))

	for _, fieldSchema := range collection.Schema.Fields {
		// timestamp field id
		if fieldSchema.FieldID == 1 || fieldSchema.IsPrimaryKey {
			fields[fieldSchema.FieldID] = fieldSchema
			continue
		}
	}
	segments, err := common.ListSegmentsVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), func(s *models.Segment) bool {
		return (p.SegmentID == 0 || p.SegmentID == s.ID) &&
			p.CollectionID == s.CollectionID &&
			(p.IncludeUnhealthy || s.State != models.SegmentStateDropped)
	})
	if err != nil {
		fmt.Println("DDD")
		return err
	}

	fmt.Println("len of segments:", len(segments))
	params := []oss.MinioConnectParam{oss.WithSkipCheckBucket(p.SkipBucketCheck)}
	if p.MinioAddress != "" {
		params = append(params, oss.WithMinioAddr(p.MinioAddress))
	}

	minioClient, bucketName, rootPath, err := s.GetMinioClientFromCfg(ctx, params...)
	if err != nil {
		fmt.Println("Failed to create client,", err.Error())
		return err
	}
	getObject := func(binlogPath string) (*minio.Object, error) {
		logPath := strings.Replace(binlogPath, "ROOT_PATH", rootPath, -1)
		return minioClient.GetObject(ctx, bucketName, logPath, minio.GetObjectOptions{})
	}

	for _, segment := range segments {
		dirPath := fmt.Sprintf("./dewu/%d/", segment.ID)
		err := os.MkdirAll(dirPath, 0755)
		if err != nil {
			fmt.Println("failed to create dir:", err)
			return err
		}
		if segment.Level != models.SegmentLevelL0 {
			err = s.downloadPrimaryKeys(ctx, segment, fields, pkField.FieldID, getObject)
			if err != nil {
				return err
			}
			// download bf files
			if err := s.downloadBFs(segment, pkField.FieldID, getObject); err != nil {
				return err
			}
		}

		// download delta files
		if err := s.downloadDeltaLogs(segment, getObject); err != nil {
			return err
		}
		return nil

	}
	return nil
}

func (s *InstanceState) downloadPrimaryKeys(ctx context.Context, segment *models.Segment, fields map[int64]models.FieldSchema, pkFieldID int64, getObject func(binlogPath string) (*minio.Object, error)) error {
	var pkBinlog *models.FieldBinlog
	targetFieldBinlogs := []*models.FieldBinlog{}
	for _, fieldBinlog := range segment.GetBinlogs() {
		_, inTarget := fields[fieldBinlog.FieldID]
		if inTarget {
			targetFieldBinlogs = append(targetFieldBinlogs, fieldBinlog)
		}
		if fieldBinlog.FieldID == pkFieldID {
			pkBinlog = fieldBinlog
		}
	}

	if pkBinlog == nil {
		return fmt.Errorf("pk Binlog not found, segment %d", segment.ID)
	}

	var entryLock sync.RWMutex
	var entries []Entry
	g, ctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, len(pkBinlog.Binlogs))

	for idx := range pkBinlog.Binlogs {
		idx := idx
		sem <- struct{}{}

		g.Go(func() error {
			defer func() { <-sem }()

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			binlog := pkBinlog.Binlogs[idx]

			pkObject, err := getObject(binlog.LogPath)
			if err != nil {
				return err
			}

			// 构建字段对象映射
			fieldObjects := make(map[int64]storage.ReadSeeker)
			for _, fieldBinlog := range targetFieldBinlogs {
				if idx >= len(fieldBinlog.Binlogs) {
					return errors.New("binlog index out of range")
				}
				targetBinlog := fieldBinlog.Binlogs[idx]
				targetObject, err := getObject(targetBinlog.LogPath)
				if err != nil {
					return err
				}
				fieldObjects[fieldBinlog.FieldID] = targetObject
			}

			var localEntries []Entry
			err = s.downloadPrimaryKs(pkObject, fieldObjects, func(pk storage.PrimaryKey, offset int, values map[int64]any) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				pkv := pk.GetValue()
				ts, ok := values[1].(int64)
				if !ok {
					return errors.New("type assertion failed for timestamp")
				}

				localEntries = append(localEntries, Entry{
					PK: pkv.(int64),
					TS: ts,
				})
				return nil
			})

			if err != nil {
				return err
			}

			entryLock.Lock()
			entries = append(entries, localEntries...)
			entryLock.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].PK < entries[j].PK
	})

	outputPath := fmt.Sprintf("./dewu/%d/%d.pk", segment.ID, segment.ID)
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create file failed: %v", err)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	for _, entry := range entries {
		if err := binary.Write(writer, binary.LittleEndian, entry.PK); err != nil {
			return fmt.Errorf("failed to insert PK: %v", err)
		}
		if err := binary.Write(writer, binary.LittleEndian, entry.TS); err != nil {
			return fmt.Errorf("failed to insert TS: %v", err)
		}
	}

	if err := outputFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %v", err)
	}
	return nil
}

func (s *InstanceState) downloadBFs(segment *models.Segment, pkFieldID int64, getObject func(binlogPath string) (*minio.Object, error)) error {
	bloomFilterFiles := []string{}
	statsType := 0
	for _, binlog := range segment.GetStatslogs() {
		if binlog.FieldID != pkFieldID {
			continue
		}
	Loop:
		for _, log := range binlog.Binlogs {
			_, logidx := path.Split(log.LogPath)
			// if special status log exist
			// only load one file
			switch logidx {
			case "1":
				bloomFilterFiles = []string{log.LogPath}
				statsType = 1
				break Loop
			default:
				bloomFilterFiles = append(bloomFilterFiles, log.LogPath)
			}
		}
	}

	// no stats log to parse, initialize a new BF
	if len(bloomFilterFiles) == 0 {
		return nil
	}
	//for ,
	for idx, binlog := range bloomFilterFiles {
		obj, err := getObject(binlog)
		if err != nil {
			fmt.Println("failed to download bf file")
			return err
		}
		f_name := ""
		if statsType == 1 {
			f_name = fmt.Sprintf("./dewu/%d/%d_%d.bf_1", segment.ID, segment.ID, idx)
		} else {
			f_name = fmt.Sprintf("./dewu/%d/%d_%d.bf_0", segment.ID, segment.ID, idx)
		}
		f, err := os.Create(f_name)
		if err != nil {
			fmt.Println("failed to open file")
			return err
		}
		w := bufio.NewWriter(f)
		r := bufio.NewReader(obj)
		_, err = io.Copy(w, r)
		if err != nil {
			fmt.Println(err.Error())
		}
		w.Flush()
		f.Close()
	}
	return nil
}

func (s *InstanceState) downloadPrimaryKs(pk storage.ReadSeeker, fields map[int64]storage.ReadSeeker, scanner func(pk storage.PrimaryKey, offset int, values map[int64]any) error) error {
	pkReader, desc, err := storage.NewBinlogReader(pk)
	if err != nil {
		return err
	}

	fieldDesc := make(map[int64]storage.DescriptorEvent)
	fieldData := make(map[int64]any)

	var readerErr error

	lo.MapValues(fields, func(r storage.ReadSeeker, k int64) *storage.BinlogReader {
		reader, desc, err := storage.NewBinlogReader(r)
		if err != nil {
			readerErr = err
			return nil
		}
		fieldDesc[k] = desc

		var data any
		switch desc.PayloadDataType {
		case schemapb.DataType_Int64:
			data, err = reader.NextInt64EventReader()
		case schemapb.DataType_VarChar:
			data, err = reader.NextVarcharEventReader()
		case schemapb.DataType_Float:
			data, err = reader.NextFloat32EventReader()
		case schemapb.DataType_Double:
			data, err = reader.NextFloat64EventReader()
		}
		if err != nil {
			readerErr = err
			return nil
		}
		fieldData[k] = data
		return reader
	})

	if readerErr != nil {
		return readerErr
	}

	var pks []storage.PrimaryKey

	switch desc.PayloadDataType {
	case schemapb.DataType_Int64:
		values, err := pkReader.NextInt64EventReader()
		if err != nil {
			return err
		}
		pks = lo.Map(values, func(id int64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(id) })
	case schemapb.DataType_VarChar:
		values, err := pkReader.NextVarcharEventReader()
		if err != nil {
			return err
		}
		pks = lo.Map(values, func(id string, _ int) storage.PrimaryKey { return storage.NewVarCharPrimaryKey(id) })
	}

	for idx, pk := range pks {
		fields := make(map[int64]any)
		for fid, data := range fieldData {
			switch fieldDesc[fid].PayloadDataType {
			case schemapb.DataType_Int64:
				values := data.([]int64)
				fields[fid] = values[idx]
			case schemapb.DataType_VarChar:
				values := data.([]string)
				fields[fid] = values[idx]
			case schemapb.DataType_Float:
				values := data.([]float32)
				fields[fid] = values[idx]
			case schemapb.DataType_Double:
				values := data.([]float64)
				fields[fid] = values[idx]
			}
		}
		err = scanner(pk, idx, fields)
		if err != nil {
			fmt.Println("scan err", err.Error())
			return err
		}
	}
	return nil
}

func (s *InstanceState) downloadDeltaLogs(segment *models.Segment, getObject func(binlogPath string) (*minio.Object, error)) error {
	deltaFiles := []string{}
	for _, binlog := range segment.GetDeltalogs() {
		for _, log := range binlog.Binlogs {
			deltaFiles = append(deltaFiles, log.LogPath)
		}
	}

	// no stats log to parse, initialize a new BF
	if len(deltaFiles) == 0 {
		return nil
	}
	//for ,
	for idx, binlog := range deltaFiles {
		obj, err := getObject(binlog)
		if err != nil {
			fmt.Println("failed to download bf file")
			return err
		}
		f_name := fmt.Sprintf("./dewu/%d/%d_%d.delta", segment.ID, segment.ID, idx)
		f, err := os.Create(f_name)
		if err != nil {
			fmt.Println("failed to open file")
			return err
		}
		w := bufio.NewWriter(f)
		r := bufio.NewReader(obj)
		_, err = io.Copy(w, r)
		if err != nil {
			fmt.Println(err.Error())
		}
		w.Flush()
		f.Close()
	}
	return nil
}
