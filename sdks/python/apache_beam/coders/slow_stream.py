#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A pure Python implementation of stream.pyx.

For internal use only; no backwards-compatibility guarantees.
"""
# pytype: skip-file

import struct

_BYTE_TABLE = [bytes((i, )) for i in range(256)]
_STRUCT_Q = struct.Struct('>q')
_STRUCT_UQ = struct.Struct('>Q')
_STRUCT_I = struct.Struct('>i')
_STRUCT_H = struct.Struct('>h')
_STRUCT_D = struct.Struct('>d')
_STRUCT_F = struct.Struct('>f')


class OutputStream(object):
  """For internal use only; no backwards-compatibility guarantees.

  A pure Python implementation of stream.OutputStream."""
  def __init__(self):
    self.data: list[bytes] = []
    self.byte_count = 0

  def write(self, b: bytes, nested: bool = False) -> None:
    assert isinstance(b, bytes)
    blen = len(b)
    if nested:
      self.write_var_int64(blen)
    self.data.append(b)
    self.byte_count += blen

  def write_byte(self, val):
    self.data.append(_BYTE_TABLE[val])
    self.byte_count += 1

  def write_var_int64(self, v: int) -> None:
    if 0 <= v <= 0x7F:
      self.data.append(_BYTE_TABLE[v])
      self.byte_count += 1
      return
    if v < 0:
      v += 1 << 64
      if v <= 0:
        raise ValueError('Value too large (negative).')
    while True:
      bits = v & 0x7F
      v >>= 7
      if v:
        bits |= 0x80
      self.data.append(_BYTE_TABLE[bits])
      self.byte_count += 1
      if not v:
        break

  def write_var_int32(self, v: int) -> None:
    self.write_var_int64(int(v) & 0xFFFFFFFF)

  def write_bigendian_int64(self, v):
    self.data.append(_STRUCT_Q.pack(v))
    self.byte_count += 8

  def write_bigendian_uint64(self, v):
    self.data.append(_STRUCT_UQ.pack(v))
    self.byte_count += 8

  def write_bigendian_int32(self, v):
    self.data.append(_STRUCT_I.pack(v))
    self.byte_count += 4

  def write_bigendian_int16(self, v):
    self.data.append(_STRUCT_H.pack(v))
    self.byte_count += 2

  def write_bigendian_double(self, v):
    self.data.append(_STRUCT_D.pack(v))
    self.byte_count += 8

  def write_bigendian_float(self, v):
    self.data.append(_STRUCT_F.pack(v))
    self.byte_count += 4

  def get(self) -> bytes:
    return b''.join(self.data)

  def size(self) -> int:
    return self.byte_count

  def _clear(self) -> None:
    self.data = []
    self.byte_count = 0


class ByteCountingOutputStream(OutputStream):
  """For internal use only; no backwards-compatibility guarantees.

  A pure Python implementation of stream.ByteCountingOutputStream."""
  def __init__(self):
    self.count = 0

  def write(self, byte_array: bytes, nested: bool = False) -> None:
    blen = len(byte_array)
    if nested:
      self.write_var_int64(blen)
    self.count += blen

  def write_byte(self, _):
    self.count += 1

  def write_var_int64(self, v: int) -> None:
    self.count += get_varint_size(v)

  def write_var_int32(self, v: int) -> None:
    if v < 0:
      self.count += 5
    else:
      self.count += get_varint_size(v)

  def write_bigendian_int64(self, _):
    self.count += 8

  def write_bigendian_uint64(self, _):
    self.count += 8

  def write_bigendian_int32(self, _):
    self.count += 4

  def write_bigendian_int16(self, _):
    self.count += 2

  def write_bigendian_double(self, _):
    self.count += 8

  def write_bigendian_float(self, _):
    self.count += 4

  def get_count(self):
    return self.count

  def size(self) -> int:
    return self.count

  def get(self):
    raise NotImplementedError

  def __str__(self):
    return '<%s %s>' % (self.__class__.__name__, self.count)


class InputStream(object):
  """For internal use only; no backwards-compatibility guarantees.

  A pure Python implementation of stream.InputStream."""
  def __init__(self, data: bytes) -> None:
    self.data = data
    self.pos = 0

  def size(self):
    return len(self.data) - self.pos

  def read(self, size: int) -> bytes:
    self.pos += size
    return self.data[self.pos - size:self.pos]

  def read_all(self, nested: bool) -> bytes:
    return self.read(self.read_var_int64() if nested else self.size())

  def read_byte(self) -> int:
    self.pos += 1
    return self.data[self.pos - 1]

  def read_var_int64(self):
    byte = self.data[self.pos]
    self.pos += 1
    if byte <= 0x7F:
      return byte
    shift = 0
    result = 0
    while True:
      if byte < 0:
        raise RuntimeError('VarLong not terminated.')

      bits = byte & 0x7F
      if shift >= 64 or (shift >= 63 and bits > 1):
        raise RuntimeError('VarLong too long.')
      result |= bits << shift
      shift += 7
      if not byte & 0x80:
        break
      byte = self.read_byte()
    if result >= 1 << 63:
      result -= 1 << 64
    return result

  def read_var_int32(self):
    v = self.read_var_int64()
    return struct.unpack('<i', struct.pack('<I', v))[0]

  def read_bigendian_int64(self):
    pos = self.pos
    self.pos = pos + 8
    return _STRUCT_Q.unpack_from(self.data, pos)[0]

  def read_bigendian_uint64(self):
    pos = self.pos
    self.pos = pos + 8
    return _STRUCT_UQ.unpack_from(self.data, pos)[0]

  def read_bigendian_int32(self):
    pos = self.pos
    self.pos = pos + 4
    return _STRUCT_I.unpack_from(self.data, pos)[0]

  def read_bigendian_int16(self):
    pos = self.pos
    self.pos = pos + 2
    return _STRUCT_H.unpack_from(self.data, pos)[0]

  def read_bigendian_double(self):
    pos = self.pos
    self.pos = pos + 8
    return _STRUCT_D.unpack_from(self.data, pos)[0]

  def read_bigendian_float(self):
    pos = self.pos
    self.pos = pos + 4
    return _STRUCT_F.unpack_from(self.data, pos)[0]


def get_varint_size(v):
  """For internal use only; no backwards-compatibility guarantees.

  Returns the size of the given integer value when encode as a VarInt."""
  if 0 <= v <= 0x7F:
    return 1
  if v < 0:
    v += 1 << 64
    if v <= 0:
      raise ValueError('Value too large (negative).')
  return (v.bit_length() + 6) // 7
