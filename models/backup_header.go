package models

import "github.com/golang/protobuf/proto"

// BackupHeader stores etcd backup header information
type BackupHeader struct {
	// Version number for backup format
	Version int32 `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	// instance name, as rootPath for key prefix
	Instance string `protobuf:"bytes,2,opt,name=instance,proto3" json:"instance,omitempty"`
	// MetaPath used in keys
	MetaPath string `protobuf:"bytes,3,opt,name=meta_path,proto3" json:"meta_path,omitempty"`
	// Entries record number of key-value in backup
	Entries int64 `protobuf:"varint,4,opt,name=entries,proto3" json:"entries,omitempty"`
	// Component is the backup target
	Component string `protobuf:"varint,5,opt,name=component,proto3" json:"component,omitempty"`
	// Extra property reserved
	Extra []byte `protobuf:"bytes,6,opt,name=extra,proto3" json:"-"`
}

// Reset implements protoiface.MessageV1
func (v *BackupHeader) Reset() {
	*v = BackupHeader{}
}

// String implements protoiface.MessageV1
func (v *BackupHeader) String() string {
	return proto.CompactTextString(v)
}

// String implements protoiface.MessageV1
func (v *BackupHeader) ProtoMessage() {}
