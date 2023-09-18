#include <assert.h>
#include <ostream>
#include <stdint.h>
#include <string_view>
#include <vector>

using Blob = std::basic_string_view<unsigned char>;

class BlobReader {
public:
  constexpr BlobReader(Blob b) : data_(b) {}
  constexpr auto size() const { return data_.size(); }
  constexpr auto offset() const { return offset_; }
  constexpr auto BytesLeft() const { return size() - offset(); }
  [[nodiscard]] constexpr bool Eof() const { return offset_ >= data_.size(); }
  [[nodiscard]] constexpr Blob ReadBytes(size_t n) {
    assert(offset_ + n <= data_.size());
    const auto ret = data_.substr(offset_, n);
    offset_ += n;
    return ret;
  }
  [[nodiscard]] constexpr auto ReadByte() { return ReadLe<uint8_t>(); }
  [[nodiscard]] constexpr auto ReadLe64() { return ReadLe<uint64_t>(); }
  [[nodiscard]] constexpr auto ReadLe32() { return ReadLe<uint32_t>(); }
  [[nodiscard]] constexpr auto ReadLe16() { return ReadLe<uint16_t>(); }
  [[nodiscard]] constexpr size_t ReadSparseInt() {
    size_t cur = ReadByte();
    size_t total = cur;
    while (cur == 0xFF) {
      cur = ReadByte();
      total += cur;
    }
    return total;
  }
  [[nodiscard]] constexpr BlobReader substr(size_t offset, size_t n) {
    return BlobReader(data_.substr(offset, n));
  }

private:
  template <typename T> [[nodiscard]] constexpr T ReadLe() {
    assert(offset_ + sizeof(T) <= data_.size());
    const auto ret =
        le32toh(*reinterpret_cast<const T *>(data_.data() + offset_));
    offset_ += sizeof(T);
    return ret;
  }
  size_t offset_ = 0;
  Blob data_;
};

static constexpr size_t DivideRoundUp(size_t a, size_t b) {
  return (a + b - 1) / b;
}

static constexpr size_t kMinMatchLength = 4;
struct Lz4Op {
  Blob literals;
  size_t offset;
  size_t match_length;
  [[nodiscard]] constexpr bool operator==(const Lz4Op &rhs) const {
    return literals == rhs.literals && offset == rhs.offset &&
           match_length == rhs.match_length;
  }
  static constexpr size_t SparseIntExtraBytes(size_t n) {
    if (n < 15) {
      return 0;
    }
    return DivideRoundUp(n - 14, 255);
  }
  static constexpr Lz4Op Decode(BlobReader *reader) {
    BlobReader &reader_ = *reader;
    if (reader_.Eof()) {
      return {{}, 0, 0};
    }
    const auto token = reader_.ReadByte();
    size_t literal_length = token >> 4;
    if (literal_length == 0xF) {
      literal_length += reader_.ReadSparseInt();
    }
    const auto literals = reader_.ReadBytes(literal_length);
    if (reader_.Eof()) {
      return {.literals = literals, .offset = 0, .match_length = 0};
    }
    const auto offset = reader_.ReadLe16();
    size_t match_length = token & ((1 << 4) - 1);
    if (match_length == 0xF) {
      match_length += reader_.ReadSparseInt();
    }
    match_length += kMinMatchLength;
    return {
        .literals = literals, .offset = offset, .match_length = match_length};
  }
  constexpr size_t EncodedSize() const {
    return 1 + SparseIntExtraBytes(literals.size()) + 2 +
           SparseIntExtraBytes(match_length - kMinMatchLength) +
           literals.size();
  }
  constexpr size_t DecodedSize() const {
    return literals.size() + match_length;
  }
};

std::ostream &operator<<(std::ostream &out, const Lz4Op &op);

struct Lz4IteratorEnd {};
struct Lz4Iterator {
  constexpr Lz4Iterator(BlobReader reader) : reader_(reader) { operator++(); }
  constexpr bool operator==(Lz4IteratorEnd) const {
    return op_.literals.size() == 0 && op_.match_length == 0;
  }
  constexpr bool operator!=(Lz4IteratorEnd) const {
    return !(*this == Lz4IteratorEnd{});
  }
  constexpr const Lz4Op &operator*() const { return op_; }
  constexpr Lz4Iterator &operator++() {
    op_ = Lz4Op::Decode(&reader_);
    return *this;
  }

private:
  Lz4Op op_{};
  BlobReader reader_;
};

constexpr size_t RoundUpPower2(const size_t n) {
  size_t res = 1;
  while (res < n) {
    res = res << 1;
  }
  return res;
}

struct Lz4SplitPoint {
  size_t compressed_offset;
  size_t decompressed_offset;
};

class Lz4BlockReader {
  struct Lz4OpReader {

    constexpr Lz4Iterator begin() const { return Lz4Iterator(reader_); }
    constexpr Lz4IteratorEnd end() const { return {}; }
    BlobReader reader_;
  };

public:
  constexpr Lz4BlockReader(BlobReader reader) : reader_(reader) {}
  constexpr Lz4OpReader ReadOps() const { return Lz4OpReader{reader_}; }
  std::vector<Lz4Op> ReadOpsVec() const;
  std::vector<Lz4SplitPoint> Split();
  std::vector<unsigned char> Decompress() const;

private:
  BlobReader reader_;
};

class Lz4FrameReader {
  static constexpr uint32_t kLegacyFrameMagic = 0x184C2102;
  static constexpr uint32_t kFrameMagic = 0x184D2204;

public:
  Lz4FrameReader(Blob data);
  const std::vector<Blob> &Blocks() const { return blocks_; }

private:
  BlobReader reader_;
  std::vector<Blob> blocks_;
};