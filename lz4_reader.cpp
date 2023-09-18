

#include "lz4_reader.h"
#include <algorithm>
#include <glog/logging.h>
#include <string.h>

std::vector<Lz4SplitPoint> Lz4BlockReader::Split() {

  std::vector<Lz4Op> ops;
  for (const auto &op : ReadOps()) {
    ops.emplace_back(op);
  }
  const std::vector<size_t> bytes_decompressed =
      [](const std::vector<Lz4Op> &ops) {
        std::vector<size_t> vec;
        vec.emplace_back(0);
        for (const auto &op : ops) {
          vec.emplace_back(vec.back() + op.DecodedSize());
        }
        return vec;
      }(ops);
  const std::vector<size_t> bytes_compressed =
      [&](const std::vector<Lz4Op> &ops) {
        std::vector<size_t> vec;
        vec.emplace_back(0);
        for (const auto &op : ops) {
          vec.emplace_back(vec.back() + op.EncodedSize());
        }
        return vec;
      }(ops);
  // for (size_t i = 0; i < bytes_compressed.size() - 1; i++) {
  //   auto op_data = reader_.substr(bytes_compressed[i],
  //   ops[i].EncodedSize()); CHECK_EQ(Lz4Op::Decode(&op_data), ops[i]);
  //   CHECK_EQ(op_data.offset(), op_data.size());
  // }

  std::vector<size_t> deps(ops.size());
  for (size_t i = 0; i < ops.size(); i++) {
    const auto &op = ops[i];
    const auto dependency_byte_offset =
        bytes_decompressed[i] + op.literals.size() - op.offset;
    auto it =
        std::upper_bound(bytes_decompressed.begin(), bytes_decompressed.end(),
                         dependency_byte_offset);
    CHECK(it != bytes_decompressed.begin());
    --it;
    deps[i] = it - bytes_decompressed.begin();
  }
  for (size_t i = deps.size() - 1; i > 0; i--) {
    deps[i - 1] = std::min(deps[i], deps[i - 1]);
  }
  std::vector<Lz4SplitPoint> ret;
  for (int i = 0; i < deps.size(); i++) {
    if (deps[i] >= i) {
      ret.emplace_back(
          Lz4SplitPoint{.compressed_offset = bytes_compressed[i],
                        .decompressed_offset = bytes_decompressed[i]});
    }
  }
  return ret;
}

std::vector<Lz4Op> Lz4BlockReader::ReadOpsVec() const {
  std::vector<Lz4Op> ops;
  for (const auto op : ReadOps()) {
    ops.emplace_back(op);
  }
  return ops;
}

std::vector<unsigned char> Lz4BlockReader::Decompress() const {
  std::vector<unsigned char> data;
  data.reserve(data.size() + reader_.size());
  for (const auto &op : ReadOps()) {
    data.insert(data.end(), op.literals.begin(), op.literals.end());
    const auto size = data.size();
    const auto dest_size = size + op.match_length;
    if (data.capacity() < dest_size) {
      data.reserve(RoundUpPower2(dest_size));
    }
    assert(op.offset <= data.size());
    data.resize(dest_size);
    if (op.match_length > op.offset) {
      for (size_t i = 0; i < op.match_length; i++) {
        data[size + i] = data[size - op.offset + i];
      }
    } else {
      memcpy(&data[size], &data[size - op.offset], op.match_length);
    }
  }
  LOG(INFO) << "Decompressed block size: " << data.size();
  return data;
}

std::vector<Blob> ParseLz4Frame(BlobReader *reader_) {
  auto &reader = *reader_;
  const auto flag = reader.ReadByte();
  // If this flag is set, a 4-bytes Dict-ID field will be present, after the
  // descriptor |flags| and the Content Size.
  const bool dictid = flag & 1;
  // Whether a 4 byte checksum is present after EndMark
  const bool csum = (flag >> 2) & 1;
  // If this flag is set, the uncompressed size of data included within the
  // frame will be present as an 8 bytes unsigned little endian value, after the
  // |flags|.
  const bool contentsize = (flag >> 3) & 1;
  // Whether a 4 byte checksum is present after each data block
  const bool bsum = (flag >> 4) & 1;
  // Whether blocks can be decoded independently
  const bool bindep = (flag >> 5) & 1;
  // Only version 01 is supposed
  const bool version = (flag >> 6) & 3;
  CHECK_EQ(version, 1);
  const auto bd = reader.ReadByte();

  const auto uncompressed_size = contentsize ? reader.ReadLe64() : 0;
  const auto dictionary_id = dictid ? reader.ReadLe32() : 0;
  const auto header_checksum = reader.ReadByte();
  std::vector<Blob> blocks;
  while (!reader.Eof()) {
    const auto block_size = reader.ReadLe32();
    // highest bit set means this block is uncompressed
    if (block_size & 0x80000000) {
      LOG(INFO) << "Detected uncompressed block of size "
                << (block_size & 0x7FFFFFFF);
      (void)reader.ReadBytes(block_size & 0x7FFFFFFF);
      continue;
    }
    if (block_size == 0) {
      break;
    }
    blocks.emplace_back(reader.ReadBytes(block_size & 0x7FFFFFFF));
    if (bsum) {
      const auto block_checksum = reader.ReadLe32();
    }
  }
  if (csum) {
    const auto content_checksum = reader.ReadLe32();
  }

  return blocks;
}

Lz4FrameReader::Lz4FrameReader(Blob data) : reader_(data) {
  const auto magic = reader_.ReadLe32();
  if (magic == kLegacyFrameMagic) {
    while (!reader_.Eof()) {
      const auto block_size = reader_.ReadLe32();
      blocks_.emplace_back(reader_.ReadBytes(block_size));
    }
  } else if (magic == kFrameMagic) {
    blocks_ = ParseLz4Frame(&reader_);
  } else {
    LOG(FATAL) << "Unrecognized magic: " << magic;
  }
}

std::ostream &operator<<(std::ostream &out, const Lz4Op &op) {
  out << "{ .literal_length = " << op.literals.size()
      << ", .offset = " << op.offset << ", .match_length = " << op.match_length
      << " }";
  return out;
}