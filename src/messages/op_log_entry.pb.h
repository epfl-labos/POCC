// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: op_log_entry.proto

#ifndef PROTOBUF_op_5flog_5fentry_2eproto__INCLUDED
#define PROTOBUF_op_5flog_5fentry_2eproto__INCLUDED

#include <string>

#include <google/protobuf/stubs/common.h>

#if GOOGLE_PROTOBUF_VERSION < 2006000
#error This file was generated by a newer version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please update
#error your headers.
#endif
#if 2006000 < GOOGLE_PROTOBUF_MIN_PROTOC_VERSION
#error This file was generated by an older version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please
#error regenerate this file with a newer version of protoc.
#endif

#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/unknown_field_set.h>
#include "rpc_messages.pb.h"
// @@protoc_insertion_point(includes)

// Internal implementation detail -- do not call these.
void  protobuf_AddDesc_op_5flog_5fentry_2eproto();
void protobuf_AssignDesc_op_5flog_5fentry_2eproto();
void protobuf_ShutdownFile_op_5flog_5fentry_2eproto();

class PbLogSetRecord;

// ===================================================================

class PbLogSetRecord : public ::google::protobuf::Message {
 public:
  PbLogSetRecord();
  virtual ~PbLogSetRecord();

  PbLogSetRecord(const PbLogSetRecord& from);

  inline PbLogSetRecord& operator=(const PbLogSetRecord& from) {
    CopyFrom(from);
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }

  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const PbLogSetRecord& default_instance();

  void Swap(PbLogSetRecord* other);

  // implements Message ----------------------------------------------

  PbLogSetRecord* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const PbLogSetRecord& from);
  void MergeFrom(const PbLogSetRecord& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:
  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // required bytes Key = 1;
  inline bool has_key() const;
  inline void clear_key();
  static const int kKeyFieldNumber = 1;
  inline const ::std::string& key() const;
  inline void set_key(const ::std::string& value);
  inline void set_key(const char* value);
  inline void set_key(const void* value, size_t size);
  inline ::std::string* mutable_key();
  inline ::std::string* release_key();
  inline void set_allocated_key(::std::string* key);

  // required bytes Value = 2;
  inline bool has_value() const;
  inline void clear_value();
  static const int kValueFieldNumber = 2;
  inline const ::std::string& value() const;
  inline void set_value(const ::std::string& value);
  inline void set_value(const char* value);
  inline void set_value(const void* value, size_t size);
  inline ::std::string* mutable_value();
  inline ::std::string* release_value();
  inline void set_allocated_value(::std::string* value);

  // required .PbPhysicalTimeSpec PUT = 3;
  inline bool has_put() const;
  inline void clear_put();
  static const int kPUTFieldNumber = 3;
  inline const ::PbPhysicalTimeSpec& put() const;
  inline ::PbPhysicalTimeSpec* mutable_put();
  inline ::PbPhysicalTimeSpec* release_put();
  inline void set_allocated_put(::PbPhysicalTimeSpec* put);

  // optional .PbPhysicalTimeSpec PDUT = 4;
  inline bool has_pdut() const;
  inline void clear_pdut();
  static const int kPDUTFieldNumber = 4;
  inline const ::PbPhysicalTimeSpec& pdut() const;
  inline ::PbPhysicalTimeSpec* mutable_pdut();
  inline ::PbPhysicalTimeSpec* release_pdut();
  inline void set_allocated_pdut(::PbPhysicalTimeSpec* pdut);

  // repeated .PbPhysicalTimeSpec DV = 5;
  inline int dv_size() const;
  inline void clear_dv();
  static const int kDVFieldNumber = 5;
  inline const ::PbPhysicalTimeSpec& dv(int index) const;
  inline ::PbPhysicalTimeSpec* mutable_dv(int index);
  inline ::PbPhysicalTimeSpec* add_dv();
  inline const ::google::protobuf::RepeatedPtrField< ::PbPhysicalTimeSpec >&
      dv() const;
  inline ::google::protobuf::RepeatedPtrField< ::PbPhysicalTimeSpec >*
      mutable_dv();

  // required int32 SrcReplica = 6;
  inline bool has_srcreplica() const;
  inline void clear_srcreplica();
  static const int kSrcReplicaFieldNumber = 6;
  inline ::google::protobuf::int32 srcreplica() const;
  inline void set_srcreplica(::google::protobuf::int32 value);

  // @@protoc_insertion_point(class_scope:PbLogSetRecord)
 private:
  inline void set_has_key();
  inline void clear_has_key();
  inline void set_has_value();
  inline void clear_has_value();
  inline void set_has_put();
  inline void clear_has_put();
  inline void set_has_pdut();
  inline void clear_has_pdut();
  inline void set_has_srcreplica();
  inline void clear_has_srcreplica();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::google::protobuf::uint32 _has_bits_[1];
  mutable int _cached_size_;
  ::std::string* key_;
  ::std::string* value_;
  ::PbPhysicalTimeSpec* put_;
  ::PbPhysicalTimeSpec* pdut_;
  ::google::protobuf::RepeatedPtrField< ::PbPhysicalTimeSpec > dv_;
  ::google::protobuf::int32 srcreplica_;
  friend void  protobuf_AddDesc_op_5flog_5fentry_2eproto();
  friend void protobuf_AssignDesc_op_5flog_5fentry_2eproto();
  friend void protobuf_ShutdownFile_op_5flog_5fentry_2eproto();

  void InitAsDefaultInstance();
  static PbLogSetRecord* default_instance_;
};
// ===================================================================


// ===================================================================

// PbLogSetRecord

// required bytes Key = 1;
inline bool PbLogSetRecord::has_key() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void PbLogSetRecord::set_has_key() {
  _has_bits_[0] |= 0x00000001u;
}
inline void PbLogSetRecord::clear_has_key() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void PbLogSetRecord::clear_key() {
  if (key_ != &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    key_->clear();
  }
  clear_has_key();
}
inline const ::std::string& PbLogSetRecord::key() const {
  // @@protoc_insertion_point(field_get:PbLogSetRecord.Key)
  return *key_;
}
inline void PbLogSetRecord::set_key(const ::std::string& value) {
  set_has_key();
  if (key_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    key_ = new ::std::string;
  }
  key_->assign(value);
  // @@protoc_insertion_point(field_set:PbLogSetRecord.Key)
}
inline void PbLogSetRecord::set_key(const char* value) {
  set_has_key();
  if (key_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    key_ = new ::std::string;
  }
  key_->assign(value);
  // @@protoc_insertion_point(field_set_char:PbLogSetRecord.Key)
}
inline void PbLogSetRecord::set_key(const void* value, size_t size) {
  set_has_key();
  if (key_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    key_ = new ::std::string;
  }
  key_->assign(reinterpret_cast<const char*>(value), size);
  // @@protoc_insertion_point(field_set_pointer:PbLogSetRecord.Key)
}
inline ::std::string* PbLogSetRecord::mutable_key() {
  set_has_key();
  if (key_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    key_ = new ::std::string;
  }
  // @@protoc_insertion_point(field_mutable:PbLogSetRecord.Key)
  return key_;
}
inline ::std::string* PbLogSetRecord::release_key() {
  clear_has_key();
  if (key_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    return NULL;
  } else {
    ::std::string* temp = key_;
    key_ = const_cast< ::std::string*>(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
    return temp;
  }
}
inline void PbLogSetRecord::set_allocated_key(::std::string* key) {
  if (key_ != &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    delete key_;
  }
  if (key) {
    set_has_key();
    key_ = key;
  } else {
    clear_has_key();
    key_ = const_cast< ::std::string*>(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  }
  // @@protoc_insertion_point(field_set_allocated:PbLogSetRecord.Key)
}

// required bytes Value = 2;
inline bool PbLogSetRecord::has_value() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void PbLogSetRecord::set_has_value() {
  _has_bits_[0] |= 0x00000002u;
}
inline void PbLogSetRecord::clear_has_value() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void PbLogSetRecord::clear_value() {
  if (value_ != &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    value_->clear();
  }
  clear_has_value();
}
inline const ::std::string& PbLogSetRecord::value() const {
  // @@protoc_insertion_point(field_get:PbLogSetRecord.Value)
  return *value_;
}
inline void PbLogSetRecord::set_value(const ::std::string& value) {
  set_has_value();
  if (value_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    value_ = new ::std::string;
  }
  value_->assign(value);
  // @@protoc_insertion_point(field_set:PbLogSetRecord.Value)
}
inline void PbLogSetRecord::set_value(const char* value) {
  set_has_value();
  if (value_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    value_ = new ::std::string;
  }
  value_->assign(value);
  // @@protoc_insertion_point(field_set_char:PbLogSetRecord.Value)
}
inline void PbLogSetRecord::set_value(const void* value, size_t size) {
  set_has_value();
  if (value_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    value_ = new ::std::string;
  }
  value_->assign(reinterpret_cast<const char*>(value), size);
  // @@protoc_insertion_point(field_set_pointer:PbLogSetRecord.Value)
}
inline ::std::string* PbLogSetRecord::mutable_value() {
  set_has_value();
  if (value_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    value_ = new ::std::string;
  }
  // @@protoc_insertion_point(field_mutable:PbLogSetRecord.Value)
  return value_;
}
inline ::std::string* PbLogSetRecord::release_value() {
  clear_has_value();
  if (value_ == &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    return NULL;
  } else {
    ::std::string* temp = value_;
    value_ = const_cast< ::std::string*>(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
    return temp;
  }
}
inline void PbLogSetRecord::set_allocated_value(::std::string* value) {
  if (value_ != &::google::protobuf::internal::GetEmptyStringAlreadyInited()) {
    delete value_;
  }
  if (value) {
    set_has_value();
    value_ = value;
  } else {
    clear_has_value();
    value_ = const_cast< ::std::string*>(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
  }
  // @@protoc_insertion_point(field_set_allocated:PbLogSetRecord.Value)
}

// required .PbPhysicalTimeSpec PUT = 3;
inline bool PbLogSetRecord::has_put() const {
  return (_has_bits_[0] & 0x00000004u) != 0;
}
inline void PbLogSetRecord::set_has_put() {
  _has_bits_[0] |= 0x00000004u;
}
inline void PbLogSetRecord::clear_has_put() {
  _has_bits_[0] &= ~0x00000004u;
}
inline void PbLogSetRecord::clear_put() {
  if (put_ != NULL) put_->::PbPhysicalTimeSpec::Clear();
  clear_has_put();
}
inline const ::PbPhysicalTimeSpec& PbLogSetRecord::put() const {
  // @@protoc_insertion_point(field_get:PbLogSetRecord.PUT)
  return put_ != NULL ? *put_ : *default_instance_->put_;
}
inline ::PbPhysicalTimeSpec* PbLogSetRecord::mutable_put() {
  set_has_put();
  if (put_ == NULL) put_ = new ::PbPhysicalTimeSpec;
  // @@protoc_insertion_point(field_mutable:PbLogSetRecord.PUT)
  return put_;
}
inline ::PbPhysicalTimeSpec* PbLogSetRecord::release_put() {
  clear_has_put();
  ::PbPhysicalTimeSpec* temp = put_;
  put_ = NULL;
  return temp;
}
inline void PbLogSetRecord::set_allocated_put(::PbPhysicalTimeSpec* put) {
  delete put_;
  put_ = put;
  if (put) {
    set_has_put();
  } else {
    clear_has_put();
  }
  // @@protoc_insertion_point(field_set_allocated:PbLogSetRecord.PUT)
}

// optional .PbPhysicalTimeSpec PDUT = 4;
inline bool PbLogSetRecord::has_pdut() const {
  return (_has_bits_[0] & 0x00000008u) != 0;
}
inline void PbLogSetRecord::set_has_pdut() {
  _has_bits_[0] |= 0x00000008u;
}
inline void PbLogSetRecord::clear_has_pdut() {
  _has_bits_[0] &= ~0x00000008u;
}
inline void PbLogSetRecord::clear_pdut() {
  if (pdut_ != NULL) pdut_->::PbPhysicalTimeSpec::Clear();
  clear_has_pdut();
}
inline const ::PbPhysicalTimeSpec& PbLogSetRecord::pdut() const {
  // @@protoc_insertion_point(field_get:PbLogSetRecord.PDUT)
  return pdut_ != NULL ? *pdut_ : *default_instance_->pdut_;
}
inline ::PbPhysicalTimeSpec* PbLogSetRecord::mutable_pdut() {
  set_has_pdut();
  if (pdut_ == NULL) pdut_ = new ::PbPhysicalTimeSpec;
  // @@protoc_insertion_point(field_mutable:PbLogSetRecord.PDUT)
  return pdut_;
}
inline ::PbPhysicalTimeSpec* PbLogSetRecord::release_pdut() {
  clear_has_pdut();
  ::PbPhysicalTimeSpec* temp = pdut_;
  pdut_ = NULL;
  return temp;
}
inline void PbLogSetRecord::set_allocated_pdut(::PbPhysicalTimeSpec* pdut) {
  delete pdut_;
  pdut_ = pdut;
  if (pdut) {
    set_has_pdut();
  } else {
    clear_has_pdut();
  }
  // @@protoc_insertion_point(field_set_allocated:PbLogSetRecord.PDUT)
}

// repeated .PbPhysicalTimeSpec DV = 5;
inline int PbLogSetRecord::dv_size() const {
  return dv_.size();
}
inline void PbLogSetRecord::clear_dv() {
  dv_.Clear();
}
inline const ::PbPhysicalTimeSpec& PbLogSetRecord::dv(int index) const {
  // @@protoc_insertion_point(field_get:PbLogSetRecord.DV)
  return dv_.Get(index);
}
inline ::PbPhysicalTimeSpec* PbLogSetRecord::mutable_dv(int index) {
  // @@protoc_insertion_point(field_mutable:PbLogSetRecord.DV)
  return dv_.Mutable(index);
}
inline ::PbPhysicalTimeSpec* PbLogSetRecord::add_dv() {
  // @@protoc_insertion_point(field_add:PbLogSetRecord.DV)
  return dv_.Add();
}
inline const ::google::protobuf::RepeatedPtrField< ::PbPhysicalTimeSpec >&
PbLogSetRecord::dv() const {
  // @@protoc_insertion_point(field_list:PbLogSetRecord.DV)
  return dv_;
}
inline ::google::protobuf::RepeatedPtrField< ::PbPhysicalTimeSpec >*
PbLogSetRecord::mutable_dv() {
  // @@protoc_insertion_point(field_mutable_list:PbLogSetRecord.DV)
  return &dv_;
}

// required int32 SrcReplica = 6;
inline bool PbLogSetRecord::has_srcreplica() const {
  return (_has_bits_[0] & 0x00000020u) != 0;
}
inline void PbLogSetRecord::set_has_srcreplica() {
  _has_bits_[0] |= 0x00000020u;
}
inline void PbLogSetRecord::clear_has_srcreplica() {
  _has_bits_[0] &= ~0x00000020u;
}
inline void PbLogSetRecord::clear_srcreplica() {
  srcreplica_ = 0;
  clear_has_srcreplica();
}
inline ::google::protobuf::int32 PbLogSetRecord::srcreplica() const {
  // @@protoc_insertion_point(field_get:PbLogSetRecord.SrcReplica)
  return srcreplica_;
}
inline void PbLogSetRecord::set_srcreplica(::google::protobuf::int32 value) {
  set_has_srcreplica();
  srcreplica_ = value;
  // @@protoc_insertion_point(field_set:PbLogSetRecord.SrcReplica)
}


// @@protoc_insertion_point(namespace_scope)

#ifndef SWIG
namespace google {
namespace protobuf {


}  // namespace google
}  // namespace protobuf
#endif  // SWIG

// @@protoc_insertion_point(global_scope)

#endif  // PROTOBUF_op_5flog_5fentry_2eproto__INCLUDED