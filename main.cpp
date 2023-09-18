
#include "lz4_reader.h"
#include <glog/logging.h>
#include <lz4.h>
#include <ostream>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

template <typename T>
std::ostream &operator<<(std::ostream &out, const std::vector<T> &vec) {
  if (vec.empty()) {
    out << "{}";
    return out;
  }
  out << '{';
  out << vec[0];
  for (size_t i = 1; i < vec.size(); i++) {
    const auto &t = vec[i];
    out << ", " << t;
  }
  out << '}';
  return out;
}

auto DecompressBlock(Blob block, bool verify = true) {
  Lz4BlockReader reader{block};
  const auto decompressed_block = reader.Decompress();
  if (!verify) {
    return decompressed_block;
  }
  std::vector<uint8_t> expected_data(decompressed_block.size());
  const auto bytes_decompressed = LZ4_decompress_safe_partial(
      reinterpret_cast<const char *>(block.data()),
      reinterpret_cast<char *>(expected_data.data()), block.size(),
      expected_data.size(), expected_data.size());
  CHECK_EQ(bytes_decompressed, decompressed_block.size());
  for (size_t i = 0; i << decompressed_block.size(); i++) {
    CHECK_EQ(decompressed_block[i], expected_data[i]);
  }
  return decompressed_block;
}

int main(int argc, const char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  fLB::FLAGS_colorlogtostderr = true;
  fLB::FLAGS_logtostderr = true;
  if (argc != 2 && argc != 1) {
    LOG(ERROR) << "Usage: " << argv[0] << " <lz4 compressed file name>";
    return 1;
  }
  const char *path = argc == 2 ? argv[1] : "Image.lz4";
  int fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    PLOG(ERROR) << "Failed to open " << path;
    return errno;
  }
  struct stat st {};
  fstat(fd, &st);
  const auto data = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  LOG(INFO) << "Mapped " << path << " to memory @ " << data;
  Lz4FrameReader reader({reinterpret_cast<unsigned char *>(data),
                         static_cast<size_t>(st.st_size)});
  int out_fd = open("kernel.bin", O_RDWR | O_CREAT | O_CLOEXEC | O_TRUNC, 0644);

  for (const auto &block : reader.Blocks()) {
    Lz4BlockReader reader{block};
    const auto decompressed_block = DecompressBlock(block);
    LOG(INFO) << "Attempting to split input block of size: " << block.size();
    const auto split_points = reader.Split();
    for (const auto &p : split_points) {
      if (p.compressed_offset == 0) {
        continue;
      }
      const auto data = block.substr(p.compressed_offset);
      std::vector<uint8_t> decompressed(decompressed_block.size() -
                                        p.decompressed_offset);
      const auto bytes_decompressed = LZ4_decompress_safe_partial(
          reinterpret_cast<const char *>(data.data()),
          reinterpret_cast<char *>(decompressed.data()), data.size(),
          decompressed.size(), decompressed.size());
      CHECK_EQ(bytes_decompressed, decompressed.size());
      LOG(INFO) << "Compressed bytes offset: " << p.compressed_offset
                << ", decompressed bytes offset: " << p.decompressed_offset;
    }
  }
  return 0;
}