# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: example.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='example.proto',
  package='',
  syntax='proto3',
  serialized_options=_b('\n!com.spark.dialect.generated.protoB\tPbExampleH\001\210\001\001\240\001\001'),
  serialized_pb=_b('\n\rexample.proto\"\\\n\x14PbExampleDataRequest\x12\x0e\n\x06taskId\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x02 \x01(\t\x12\x11\n\tbatchSize\x18\x03 \x01(\x03\x12\x11\n\tbatchData\x18\x04 \x01(\x0c\"=\n\x17PbExampleComputeRequest\x12\x0e\n\x06taskId\x18\x01 \x01(\t\x12\x12\n\ntargetFile\x18\x02 \x01(\t\"v\n\x11PbExampleResponse\x12)\n\x06status\x18\x01 \x01(\x0e\x32\x19.PbExampleResponse.STATUS\x12\x0f\n\x07message\x18\x02 \x01(\t\"%\n\x06STATUS\x12\x06\n\x02OK\x10\x00\x12\x08\n\x04\x42USY\x10\x01\x12\t\n\x05\x45RROR\x10\x02\x32\x83\x01\n\x10PbExampleService\x12\x36\n\tsendBatch\x12\x15.PbExampleDataRequest\x1a\x12.PbExampleResponse\x12\x37\n\x07\x63ompute\x12\x18.PbExampleComputeRequest\x1a\x12.PbExampleResponseB6\n!com.spark.dialect.generated.protoB\tPbExampleH\x01\x88\x01\x01\xa0\x01\x01\x62\x06proto3')
)



_PBEXAMPLERESPONSE_STATUS = _descriptor.EnumDescriptor(
  name='STATUS',
  full_name='PbExampleResponse.STATUS',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='OK', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='BUSY', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ERROR', index=2, number=2,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=255,
  serialized_end=292,
)
_sym_db.RegisterEnumDescriptor(_PBEXAMPLERESPONSE_STATUS)


_PBEXAMPLEDATAREQUEST = _descriptor.Descriptor(
  name='PbExampleDataRequest',
  full_name='PbExampleDataRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='taskId', full_name='PbExampleDataRequest.taskId', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='schema', full_name='PbExampleDataRequest.schema', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='batchSize', full_name='PbExampleDataRequest.batchSize', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='batchData', full_name='PbExampleDataRequest.batchData', index=3,
      number=4, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=17,
  serialized_end=109,
)


_PBEXAMPLECOMPUTEREQUEST = _descriptor.Descriptor(
  name='PbExampleComputeRequest',
  full_name='PbExampleComputeRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='taskId', full_name='PbExampleComputeRequest.taskId', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='targetFile', full_name='PbExampleComputeRequest.targetFile', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=111,
  serialized_end=172,
)


_PBEXAMPLERESPONSE = _descriptor.Descriptor(
  name='PbExampleResponse',
  full_name='PbExampleResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='PbExampleResponse.status', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='message', full_name='PbExampleResponse.message', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _PBEXAMPLERESPONSE_STATUS,
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=174,
  serialized_end=292,
)

_PBEXAMPLERESPONSE.fields_by_name['status'].enum_type = _PBEXAMPLERESPONSE_STATUS
_PBEXAMPLERESPONSE_STATUS.containing_type = _PBEXAMPLERESPONSE
DESCRIPTOR.message_types_by_name['PbExampleDataRequest'] = _PBEXAMPLEDATAREQUEST
DESCRIPTOR.message_types_by_name['PbExampleComputeRequest'] = _PBEXAMPLECOMPUTEREQUEST
DESCRIPTOR.message_types_by_name['PbExampleResponse'] = _PBEXAMPLERESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

PbExampleDataRequest = _reflection.GeneratedProtocolMessageType('PbExampleDataRequest', (_message.Message,), {
  'DESCRIPTOR' : _PBEXAMPLEDATAREQUEST,
  '__module__' : 'example_pb2'
  # @@protoc_insertion_point(class_scope:PbExampleDataRequest)
  })
_sym_db.RegisterMessage(PbExampleDataRequest)

PbExampleComputeRequest = _reflection.GeneratedProtocolMessageType('PbExampleComputeRequest', (_message.Message,), {
  'DESCRIPTOR' : _PBEXAMPLECOMPUTEREQUEST,
  '__module__' : 'example_pb2'
  # @@protoc_insertion_point(class_scope:PbExampleComputeRequest)
  })
_sym_db.RegisterMessage(PbExampleComputeRequest)

PbExampleResponse = _reflection.GeneratedProtocolMessageType('PbExampleResponse', (_message.Message,), {
  'DESCRIPTOR' : _PBEXAMPLERESPONSE,
  '__module__' : 'example_pb2'
  # @@protoc_insertion_point(class_scope:PbExampleResponse)
  })
_sym_db.RegisterMessage(PbExampleResponse)


DESCRIPTOR._options = None

_PBEXAMPLESERVICE = _descriptor.ServiceDescriptor(
  name='PbExampleService',
  full_name='PbExampleService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=295,
  serialized_end=426,
  methods=[
  _descriptor.MethodDescriptor(
    name='sendBatch',
    full_name='PbExampleService.sendBatch',
    index=0,
    containing_service=None,
    input_type=_PBEXAMPLEDATAREQUEST,
    output_type=_PBEXAMPLERESPONSE,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='compute',
    full_name='PbExampleService.compute',
    index=1,
    containing_service=None,
    input_type=_PBEXAMPLECOMPUTEREQUEST,
    output_type=_PBEXAMPLERESPONSE,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_PBEXAMPLESERVICE)

DESCRIPTOR.services_by_name['PbExampleService'] = _PBEXAMPLESERVICE

# @@protoc_insertion_point(module_scope)
