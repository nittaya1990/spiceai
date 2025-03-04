// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.17.3
// source: proto/runtime/v1/runtime.proto

package runtime_pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ExportModel struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Directory string `protobuf:"bytes,1,opt,name=directory,proto3" json:"directory,omitempty"`
	Filename  string `protobuf:"bytes,2,opt,name=filename,proto3" json:"filename,omitempty"`
}

func (x *ExportModel) Reset() {
	*x = ExportModel{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_runtime_v1_runtime_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExportModel) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExportModel) ProtoMessage() {}

func (x *ExportModel) ProtoReflect() protoreflect.Message {
	mi := &file_proto_runtime_v1_runtime_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExportModel.ProtoReflect.Descriptor instead.
func (*ExportModel) Descriptor() ([]byte, []int) {
	return file_proto_runtime_v1_runtime_proto_rawDescGZIP(), []int{0}
}

func (x *ExportModel) GetDirectory() string {
	if x != nil {
		return x.Directory
	}
	return ""
}

func (x *ExportModel) GetFilename() string {
	if x != nil {
		return x.Filename
	}
	return ""
}

type ImportModel struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Pod         string `protobuf:"bytes,1,opt,name=pod,proto3" json:"pod,omitempty"`
	Tag         string `protobuf:"bytes,2,opt,name=tag,proto3" json:"tag,omitempty"`
	ArchivePath string `protobuf:"bytes,3,opt,name=archive_path,json=archivePath,proto3" json:"archive_path,omitempty"`
}

func (x *ImportModel) Reset() {
	*x = ImportModel{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_runtime_v1_runtime_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ImportModel) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ImportModel) ProtoMessage() {}

func (x *ImportModel) ProtoReflect() protoreflect.Message {
	mi := &file_proto_runtime_v1_runtime_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ImportModel.ProtoReflect.Descriptor instead.
func (*ImportModel) Descriptor() ([]byte, []int) {
	return file_proto_runtime_v1_runtime_proto_rawDescGZIP(), []int{1}
}

func (x *ImportModel) GetPod() string {
	if x != nil {
		return x.Pod
	}
	return ""
}

func (x *ImportModel) GetTag() string {
	if x != nil {
		return x.Tag
	}
	return ""
}

func (x *ImportModel) GetArchivePath() string {
	if x != nil {
		return x.ArchivePath
	}
	return ""
}

type Episode struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Episode      uint64            `protobuf:"varint,1,opt,name=episode,proto3" json:"episode,omitempty"`
	Start        int64             `protobuf:"varint,2,opt,name=start,proto3" json:"start,omitempty"`
	End          int64             `protobuf:"varint,3,opt,name=end,proto3" json:"end,omitempty"`
	Score        float64           `protobuf:"fixed64,4,opt,name=score,proto3" json:"score,omitempty"`
	ActionsTaken map[string]uint64 `protobuf:"bytes,5,rep,name=actions_taken,json=actionsTaken,proto3" json:"actions_taken,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	Error        string            `protobuf:"bytes,6,opt,name=error,proto3" json:"error,omitempty"`
	ErrorMessage string            `protobuf:"bytes,7,opt,name=error_message,json=errorMessage,proto3" json:"error_message,omitempty"`
}

func (x *Episode) Reset() {
	*x = Episode{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_runtime_v1_runtime_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Episode) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Episode) ProtoMessage() {}

func (x *Episode) ProtoReflect() protoreflect.Message {
	mi := &file_proto_runtime_v1_runtime_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Episode.ProtoReflect.Descriptor instead.
func (*Episode) Descriptor() ([]byte, []int) {
	return file_proto_runtime_v1_runtime_proto_rawDescGZIP(), []int{2}
}

func (x *Episode) GetEpisode() uint64 {
	if x != nil {
		return x.Episode
	}
	return 0
}

func (x *Episode) GetStart() int64 {
	if x != nil {
		return x.Start
	}
	return 0
}

func (x *Episode) GetEnd() int64 {
	if x != nil {
		return x.End
	}
	return 0
}

func (x *Episode) GetScore() float64 {
	if x != nil {
		return x.Score
	}
	return 0
}

func (x *Episode) GetActionsTaken() map[string]uint64 {
	if x != nil {
		return x.ActionsTaken
	}
	return nil
}

func (x *Episode) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *Episode) GetErrorMessage() string {
	if x != nil {
		return x.ErrorMessage
	}
	return ""
}

type Flight struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Start    int64      `protobuf:"varint,1,opt,name=start,proto3" json:"start,omitempty"`
	End      int64      `protobuf:"varint,2,opt,name=end,proto3" json:"end,omitempty"`
	Episodes []*Episode `protobuf:"bytes,3,rep,name=episodes,proto3" json:"episodes,omitempty"`
}

func (x *Flight) Reset() {
	*x = Flight{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_runtime_v1_runtime_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Flight) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Flight) ProtoMessage() {}

func (x *Flight) ProtoReflect() protoreflect.Message {
	mi := &file_proto_runtime_v1_runtime_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Flight.ProtoReflect.Descriptor instead.
func (*Flight) Descriptor() ([]byte, []int) {
	return file_proto_runtime_v1_runtime_proto_rawDescGZIP(), []int{3}
}

func (x *Flight) GetStart() int64 {
	if x != nil {
		return x.Start
	}
	return 0
}

func (x *Flight) GetEnd() int64 {
	if x != nil {
		return x.End
	}
	return 0
}

func (x *Flight) GetEpisodes() []*Episode {
	if x != nil {
		return x.Episodes
	}
	return nil
}

type Pod struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name         string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	ManifestPath string `protobuf:"bytes,2,opt,name=manifest_path,json=manifestPath,proto3" json:"manifest_path,omitempty"`
}

func (x *Pod) Reset() {
	*x = Pod{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_runtime_v1_runtime_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Pod) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Pod) ProtoMessage() {}

func (x *Pod) ProtoReflect() protoreflect.Message {
	mi := &file_proto_runtime_v1_runtime_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Pod.ProtoReflect.Descriptor instead.
func (*Pod) Descriptor() ([]byte, []int) {
	return file_proto_runtime_v1_runtime_proto_rawDescGZIP(), []int{4}
}

func (x *Pod) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Pod) GetManifestPath() string {
	if x != nil {
		return x.ManifestPath
	}
	return ""
}

type Interpretation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Start   int64    `protobuf:"varint,1,opt,name=start,proto3" json:"start,omitempty"`
	End     int64    `protobuf:"varint,2,opt,name=end,proto3" json:"end,omitempty"`
	Name    string   `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Actions []string `protobuf:"bytes,4,rep,name=actions,proto3" json:"actions,omitempty"`
	Tags    []string `protobuf:"bytes,5,rep,name=tags,proto3" json:"tags,omitempty"`
}

func (x *Interpretation) Reset() {
	*x = Interpretation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_runtime_v1_runtime_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Interpretation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Interpretation) ProtoMessage() {}

func (x *Interpretation) ProtoReflect() protoreflect.Message {
	mi := &file_proto_runtime_v1_runtime_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Interpretation.ProtoReflect.Descriptor instead.
func (*Interpretation) Descriptor() ([]byte, []int) {
	return file_proto_runtime_v1_runtime_proto_rawDescGZIP(), []int{5}
}

func (x *Interpretation) GetStart() int64 {
	if x != nil {
		return x.Start
	}
	return 0
}

func (x *Interpretation) GetEnd() int64 {
	if x != nil {
		return x.End
	}
	return 0
}

func (x *Interpretation) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Interpretation) GetActions() []string {
	if x != nil {
		return x.Actions
	}
	return nil
}

func (x *Interpretation) GetTags() []string {
	if x != nil {
		return x.Tags
	}
	return nil
}

var File_proto_runtime_v1_runtime_proto protoreflect.FileDescriptor

var file_proto_runtime_v1_runtime_proto_rawDesc = []byte{
	0x0a, 0x1e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x72, 0x75, 0x6e, 0x74, 0x69, 0x6d, 0x65, 0x2f,
	0x76, 0x31, 0x2f, 0x72, 0x75, 0x6e, 0x74, 0x69, 0x6d, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x07, 0x72, 0x75, 0x6e, 0x74, 0x69, 0x6d, 0x65, 0x22, 0x47, 0x0a, 0x0b, 0x45, 0x78, 0x70,
	0x6f, 0x72, 0x74, 0x4d, 0x6f, 0x64, 0x65, 0x6c, 0x12, 0x1c, 0x0a, 0x09, 0x64, 0x69, 0x72, 0x65,
	0x63, 0x74, 0x6f, 0x72, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x64, 0x69, 0x72,
	0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x12, 0x1a, 0x0a, 0x08, 0x66, 0x69, 0x6c, 0x65, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x66, 0x69, 0x6c, 0x65, 0x6e, 0x61,
	0x6d, 0x65, 0x22, 0x54, 0x0a, 0x0b, 0x49, 0x6d, 0x70, 0x6f, 0x72, 0x74, 0x4d, 0x6f, 0x64, 0x65,
	0x6c, 0x12, 0x10, 0x0a, 0x03, 0x70, 0x6f, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x70, 0x6f, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x74, 0x61, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x03, 0x74, 0x61, 0x67, 0x12, 0x21, 0x0a, 0x0c, 0x61, 0x72, 0x63, 0x68, 0x69, 0x76, 0x65,
	0x5f, 0x70, 0x61, 0x74, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x61, 0x72, 0x63,
	0x68, 0x69, 0x76, 0x65, 0x50, 0x61, 0x74, 0x68, 0x22, 0xa6, 0x02, 0x0a, 0x07, 0x45, 0x70, 0x69,
	0x73, 0x6f, 0x64, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x65, 0x70, 0x69, 0x73, 0x6f, 0x64, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x65, 0x70, 0x69, 0x73, 0x6f, 0x64, 0x65, 0x12, 0x14,
	0x0a, 0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x73,
	0x74, 0x61, 0x72, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x03, 0x65, 0x6e, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x73, 0x63, 0x6f, 0x72, 0x65, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x01, 0x52, 0x05, 0x73, 0x63, 0x6f, 0x72, 0x65, 0x12, 0x47, 0x0a, 0x0d,
	0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x5f, 0x74, 0x61, 0x6b, 0x65, 0x6e, 0x18, 0x05, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x22, 0x2e, 0x72, 0x75, 0x6e, 0x74, 0x69, 0x6d, 0x65, 0x2e, 0x45, 0x70,
	0x69, 0x73, 0x6f, 0x64, 0x65, 0x2e, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x54, 0x61, 0x6b,
	0x65, 0x6e, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0c, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x54, 0x61, 0x6b, 0x65, 0x6e, 0x12, 0x14, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x06,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x23, 0x0a, 0x0d, 0x65,
	0x72, 0x72, 0x6f, 0x72, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x07, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0c, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x1a, 0x3f, 0x0a, 0x11, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x54, 0x61, 0x6b, 0x65, 0x6e,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38,
	0x01, 0x22, 0x5e, 0x0a, 0x06, 0x46, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x73,
	0x74, 0x61, 0x72, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x73, 0x74, 0x61, 0x72,
	0x74, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x03,
	0x65, 0x6e, 0x64, 0x12, 0x2c, 0x0a, 0x08, 0x65, 0x70, 0x69, 0x73, 0x6f, 0x64, 0x65, 0x73, 0x18,
	0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x72, 0x75, 0x6e, 0x74, 0x69, 0x6d, 0x65, 0x2e,
	0x45, 0x70, 0x69, 0x73, 0x6f, 0x64, 0x65, 0x52, 0x08, 0x65, 0x70, 0x69, 0x73, 0x6f, 0x64, 0x65,
	0x73, 0x22, 0x3e, 0x0a, 0x03, 0x50, 0x6f, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x23, 0x0a, 0x0d,
	0x6d, 0x61, 0x6e, 0x69, 0x66, 0x65, 0x73, 0x74, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0c, 0x6d, 0x61, 0x6e, 0x69, 0x66, 0x65, 0x73, 0x74, 0x50, 0x61, 0x74,
	0x68, 0x22, 0x7a, 0x0a, 0x0e, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x70, 0x72, 0x65, 0x74, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x14, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x03, 0x65, 0x6e, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12,
	0x18, 0x0a, 0x07, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x07, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x61, 0x67,
	0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73, 0x42, 0x31, 0x5a,
	0x2f, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x73, 0x70, 0x69, 0x63,
	0x65, 0x61, 0x69, 0x2f, 0x73, 0x70, 0x69, 0x63, 0x65, 0x61, 0x69, 0x2f, 0x70, 0x6b, 0x67, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x72, 0x75, 0x6e, 0x74, 0x69, 0x6d, 0x65, 0x5f, 0x70, 0x62,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_runtime_v1_runtime_proto_rawDescOnce sync.Once
	file_proto_runtime_v1_runtime_proto_rawDescData = file_proto_runtime_v1_runtime_proto_rawDesc
)

func file_proto_runtime_v1_runtime_proto_rawDescGZIP() []byte {
	file_proto_runtime_v1_runtime_proto_rawDescOnce.Do(func() {
		file_proto_runtime_v1_runtime_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_runtime_v1_runtime_proto_rawDescData)
	})
	return file_proto_runtime_v1_runtime_proto_rawDescData
}

var file_proto_runtime_v1_runtime_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_proto_runtime_v1_runtime_proto_goTypes = []interface{}{
	(*ExportModel)(nil),    // 0: runtime.ExportModel
	(*ImportModel)(nil),    // 1: runtime.ImportModel
	(*Episode)(nil),        // 2: runtime.Episode
	(*Flight)(nil),         // 3: runtime.Flight
	(*Pod)(nil),            // 4: runtime.Pod
	(*Interpretation)(nil), // 5: runtime.Interpretation
	nil,                    // 6: runtime.Episode.ActionsTakenEntry
}
var file_proto_runtime_v1_runtime_proto_depIdxs = []int32{
	6, // 0: runtime.Episode.actions_taken:type_name -> runtime.Episode.ActionsTakenEntry
	2, // 1: runtime.Flight.episodes:type_name -> runtime.Episode
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_proto_runtime_v1_runtime_proto_init() }
func file_proto_runtime_v1_runtime_proto_init() {
	if File_proto_runtime_v1_runtime_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_runtime_v1_runtime_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExportModel); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_runtime_v1_runtime_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ImportModel); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_runtime_v1_runtime_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Episode); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_runtime_v1_runtime_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Flight); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_runtime_v1_runtime_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Pod); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_runtime_v1_runtime_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Interpretation); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_runtime_v1_runtime_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_runtime_v1_runtime_proto_goTypes,
		DependencyIndexes: file_proto_runtime_v1_runtime_proto_depIdxs,
		MessageInfos:      file_proto_runtime_v1_runtime_proto_msgTypes,
	}.Build()
	File_proto_runtime_v1_runtime_proto = out.File
	file_proto_runtime_v1_runtime_proto_rawDesc = nil
	file_proto_runtime_v1_runtime_proto_goTypes = nil
	file_proto_runtime_v1_runtime_proto_depIdxs = nil
}
