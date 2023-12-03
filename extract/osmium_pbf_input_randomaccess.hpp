#ifndef OSMIUM_IO_PBF_INPUT_RANDOMACCESS_HPP
#define OSMIUM_IO_PBF_INPUT_RANDOMACCESS_HPP

/*

This file really should be part of Osmium (https://osmcode.org/libosmium), but is being rejected due to weird misunderstandings.
See https://github.com/osmcode/libosmium/pull/367

Copyright 2023 Ben Wiederhake

GNU Affero GPL, like the rest of monitor-osm-domain.

For the purpose of upstreaming this code into libosmium, I'd be happy to relicense it under the Boost Software License, like the rest of libosmium.

*/

/**
 * @file
 *
 * Include this file if you want to read OSM PBF files out of order, also called random access.
 *
 * @attention If you include this file, you'll need to link with
 *            `libz`, and enable multithreading.
 */

#include "osmium_buffer.hpp" // Inject header

#include <osmium/io/detail/pbf_input_format.hpp>
#include <osmium/io/detail/pbf_decoder.hpp>
#include <osmium/util/file.hpp>

#include <protozero/pbf_message.hpp>
#include <protozero/types.hpp>

#include <random>
#include <string>
#include <unordered_map>

namespace osmium {

    namespace util {

        /**
         * Set current offset into file.
         *
         * @param fd Open file descriptor.
         * @param offset Desired absolute offset into the file
         */
        // The fate of PR 368 is unclear. Let's define our own instead of relying on libosmium.
        // https://github.com/osmcode/libosmium/pull/368
        inline void file_seek_368(int fd, size_t offset) noexcept {
#ifdef _MSC_VER
            osmium::detail::disable_invalid_parameter_handler diph;
            // https://msdn.microsoft.com/en-us/library/1yee101t.aspx
            _lseeki64(fd, static_cast<__int64>(offset), SEEK_SET);
#else
            ::lseek(fd, offset, SEEK_SET);
#endif
        }

    } // namespace util

    namespace io {

        namespace detail {

            /* BlobHeaders without indexdata are usually only 13-14 bytes. */
            static constexpr uint32_t max_small_blob_header_size {64};

            /* Blocks are usually around 60 KiB - 500 KiB, so anything about 20 MiB is suspicious. */
            static constexpr size_t max_block_size {20 * 1024 * 1024};

            static uint32_t check_small_size(uint32_t size) {
                if (size > static_cast<uint32_t>(max_small_blob_header_size)) {
                    throw osmium::pbf_error{"invalid small BlobHeader size (> max_small_blob_header_size)"};
                }
                return size;
            }

            /* The following functions are largely copied from PBFParser, since these methods are private and cannot be re-used otherwise.
             * TODO: Implement these functions only once. */

            static uint32_t get_size_in_network_byte_order(const char* d) noexcept {
                return (static_cast<uint32_t>(d[3])) |
                       (static_cast<uint32_t>(d[2]) <<  8U) |
                       (static_cast<uint32_t>(d[1]) << 16U) |
                       (static_cast<uint32_t>(d[0]) << 24U);
            }

            /**
             * Read exactly size bytes from fd into buffer.
             *
             * @pre Value in size parameter must fit in unsigned int
             * @returns true if size bytes could be read
             *          false if EOF was encountered
             */
            static bool read_exactly(int fd, char* buffer, std::size_t size) {
                std::size_t to_read = size;

                while (to_read > 0) {
                    auto const read_size = osmium::io::detail::reliable_read(fd, buffer + (size - to_read), static_cast<unsigned int>(to_read));
                    if (read_size == 0) { // EOF
                        return false;
                    }
                    to_read -= read_size;
                }

                return true;
            }

            /**
             * Read 4 bytes in network byte order from file. They contain
             * the length of the following BlobHeader.
             */
            uint32_t read_blob_header_size_from_file(int fd) {
                std::array<char, sizeof(uint32_t)> buffer{};
                if (!detail::read_exactly(fd, buffer.data(), buffer.size())) {
                    throw osmium::pbf_error{"unexpected EOF in blob header size"};
                }
                return detail::check_small_size(detail::get_size_in_network_byte_order(buffer.data()));
            }

            /**
             * Decode the BlobHeader. Make sure it contains the expected
             * type. Return the size of the following Blob.
             */
            static size_t decode_blob_header(const protozero::data_view& data, const char* expected_type) {
                protozero::pbf_message<FileFormat::BlobHeader> pbf_blob_header{data};
                protozero::data_view blob_header_type;
                size_t blob_header_datasize = 0;

                while (pbf_blob_header.next()) {
                    switch (pbf_blob_header.tag_and_type()) {
                        case protozero::tag_and_type(FileFormat::BlobHeader::required_string_type, protozero::pbf_wire_type::length_delimited):
                            blob_header_type = pbf_blob_header.get_view();
                            break;
                        case protozero::tag_and_type(FileFormat::BlobHeader::required_int32_datasize, protozero::pbf_wire_type::varint):
                            blob_header_datasize = pbf_blob_header.get_int32();
                            break;
                        default:
                            pbf_blob_header.skip();
                    }
                }

                if (blob_header_datasize == 0) {
                    throw osmium::pbf_error{"PBF format error: BlobHeader.datasize missing or zero."};
                }

                if (std::strncmp(expected_type, blob_header_type.data(), blob_header_type.size()) != 0) {
                    throw osmium::pbf_error{"blob does not have expected type (OSMHeader in first blob, OSMData in following blobs)"};
                }

                return blob_header_datasize;
            }

            /**
             * Given an interval of size at least two, return an index somewhere in the middle.
             * This may seem trivial, but this is often enough a source of bugs, and this way it can be easily tested.
             *
             * @pre exclusive_end - inclusive_start >= 2
             * @returns an index in the middle of the indicated interval
             */
            static size_t binsearch_middle(size_t inclusive_start, size_t exclusive_end) {
                assert(exclusive_end - inclusive_start >= 2);
                return inclusive_start + (exclusive_end - inclusive_start) / 2;
            }

            /* TODO: This should live somewhere else. */
            static int compare_by_type_then_id(
                    const osmium::item_type lhs_item_type,
                    const osmium::object_id_type lhs_item_id,
                    const osmium::item_type rhs_item_type,
                    const osmium::object_id_type rhs_item_id
            ) {
                auto lhs_nwr = osmium::item_type_to_nwr_index(lhs_item_type);
                auto rhs_nwr = osmium::item_type_to_nwr_index(rhs_item_type);
                if (lhs_nwr < rhs_nwr) {
                    return -1;
                }
                if (lhs_nwr > rhs_nwr) {
                    return +1;
                }
                if (lhs_item_id < rhs_item_id) {
                    return -1;
                }
                if (lhs_item_id > rhs_item_id) {
                    return +1;
                }
                return 0;
            }

        } // namespace detail

        struct pbf_block_start {
            size_t file_offset;
            osmium::object_id_type first_item_id_or_zero;
            uint32_t datasize;
            // "first_item_type_or_zero" and "first_item_id_or_zero" are zero if that block has never been read before.
            osmium::item_type first_item_type_or_zero;
            // The weird order avoids silly padding in the struct (2 bytes instead of 10).

            bool is_populated() const {
                return first_item_type_or_zero != osmium::item_type::undefined;
            }

            bool is_needle_definitely_before(osmium::item_type needle_item_type, osmium::object_id_type needle_item_id) const {
                // No actual OSMObject available.
                if (!is_populated()) {
                    return false;
                }
                int cmp = osmium::io::detail::compare_by_type_then_id(needle_item_type, needle_item_id, first_item_type_or_zero, first_item_id_or_zero);
                return cmp < 0;
            }

        }; // struct pbf_block_start

        class PbfBlockIndexTable {
            std::vector<pbf_block_start> m_block_starts;
            int m_fd;

            size_t digest_and_skip_block(size_t current_offset, std::size_t file_size, bool should_index_block) {
                uint32_t blob_header_size = detail::read_blob_header_size_from_file(m_fd);
                current_offset += 4;

                std::string buffer;
                buffer.resize(blob_header_size);
                if (!detail::read_exactly(m_fd, &*buffer.begin(), blob_header_size)) {
                    throw osmium::pbf_error{"unexpected EOF in blob header"};
                }
                current_offset += blob_header_size;

                const char* expected_type = should_index_block ? "OSMData" : "OSMHeader";
                size_t blob_body_size = detail::decode_blob_header(protozero::data_view{buffer.data(), blob_header_size}, expected_type);
                // TODO: Check for "Sort.Type_then_ID" in optional_features, if desired.
                // (Planet has it, most extracts have it, but test data doesn't have it.)
                if (blob_body_size > detail::max_block_size) {
                    throw osmium::pbf_error{"invalid Block size (> max_block_size)"};
                }
                if (should_index_block) {
                    m_block_starts.emplace_back(pbf_block_start {
                        current_offset, // file_offset
                        0, // first_item_id_or_zero
                        static_cast<uint32_t>(blob_body_size), // block_datasize
                        osmium::item_type::undefined // first_item_type_or_zero
                    });
                }

                current_offset += blob_body_size;
                osmium::util::file_seek_368(m_fd, current_offset);
                assert(current_offset <= file_size);
                return current_offset;
            }

        public:
            /**
             * Open and index the given pbf file for future random access. This reads every block
             * *header* (not body) in the file, and allocates roughly 24 bytes for each data block.
             * Usually this scan is extremely quick. For reference, planet has roughly 50k blocks at
             * the time of writing, which means only roughly 1 MiB of index data.
             *
             * If size_t is only 32 bits, then this will fail for files bigger than 2 GiB.
             *
             * Note that io::Reader cannot be used, since it buffers, insists on parsing and
             * decompressing all blocks, and probably breaks due to seek().
             */
            PbfBlockIndexTable(const std::string& filename)
            {
                /* As we expect a reasonably large amount of entries, avoid unnecessary
                 * reallocations in the beginning: */
                m_block_starts.reserve(1000);
                m_fd = osmium::io::detail::open_for_reading(filename);
                /* TODO: Use a 64-bit interface here */
                /* TODO: At least check whether the file is larger than 2 GB and abort if 32-bit platform. */
                std::size_t file_size = osmium::util::file_size(m_fd);

                std::size_t offset = digest_and_skip_block(0, file_size, false); // HeaderBlock
                /* Data blocks, if any: */
                while (offset < file_size) {
                    offset = digest_and_skip_block(offset, file_size, true);
                }
                /* If this is a 32-bit platform and the file is larger than 2 GiB, then there is a
                 * *chance* we can detect the problem by observing a seeming read past the end, e.g.
                 * if the file size is 4GB + 1234 bytes, which causes file_size to be just 1234:
                 */
                if (offset > file_size) {
                    throw osmium::pbf_error{"file either grew, or otherwise did not have expected size (perhaps 32-bit truncation?)"};
                }
            }

            ~PbfBlockIndexTable() {
                osmium::io::detail::reliable_close(m_fd);
            }

            const std::vector<pbf_block_start>& block_starts() const {
                return m_block_starts;
            }

            /**
             * Reads and parses a block into a given buffer. Note that this class does not cache
             * recently-accessed blocks, and thus cannot be used in parallel.
             *
             * @pre block_index must be a valid index into m_block_starts.
             * @returns The decoded block
             */
            std::vector<std::unique_ptr<osmium::memory::Buffer>> get_parsed_block(size_t block_index, const osmium::io::read_meta read_metadata) {
                /* Because we might need to read the block to update m_block_starts, *all* item types must be decoded. This should not be a problem anyway, because the block likely only contains items of the desired type, as items should be sorted first by type, then by ID. */
                const osmium::osm_entity_bits::type read_types = osmium::osm_entity_bits::all;

                auto& block_start = m_block_starts[block_index];
                /* Because of the write-access to m_block_starts and file seeking, this cannot be
                 * easily parallelized. */
                osmium::util::file_seek_368(m_fd, block_start.file_offset);

                std::string input_buffer;
                input_buffer.resize(block_start.datasize);
                if (!osmium::io::detail::read_exactly(m_fd, &*input_buffer.begin(), block_start.datasize)) {
                    throw osmium::pbf_error{"unexpected EOF"};
                }
                /* auto_grow::internal leads to linked lists, and iterating it will be "accidentally quadratic".
                 * While this may be okay for linear scans, it makes it unnecessarily difficult to quickly check the first item in the buffer.
                 * Thus, override it to auto_grow::yes, which results in a single, huge, but *contiguous* buffer. */
                osmium::io::detail::PBFDataBlobDecoder data_blob_parser{std::move(input_buffer), read_types, read_metadata};

                std::vector<std::unique_ptr<osmium::memory::Buffer>> buffers = data_blob_parser().extract_nested_buffers();

                if (!block_start.is_populated()) {
                    const osmium::memory::Buffer& buffer = *buffers.back();
                    auto it = buffer.begin<osmium::OSMObject>();
                    if (it != buffer.end<osmium::OSMObject>()) {
                        block_start.first_item_id_or_zero = it->id();
                        block_start.first_item_type_or_zero = it->type();
                    }
                }
                return buffers;
            }

            /**
             * Execute a binary search for the "needle" OSMObject, assuming that the data are sorted by type first, and ID second.
             *
             * This is a very low-level function that allows easily intercepting *all* decompressed buffers, even those that are just speculative. If you need a simpler interface, see FIXME.
             *
             * - begin_search and end_search are the inclusive and exclusive ends of the interval to be searched, and are updated by this method.
             * - If we can conclusively prove that no such block exists (because the needle has a smaller type+ID than even the first block, or because begin_search == end_search), then the return value is this->block_starts().size().
             * - Otherwise, the return value is the index of a block that might contain the needle. If the block has never been read before, it is possible that the needle exists in a different block.
             *
             * Proceed in three stages:
             * 1. Do a binary search, hoping that all blocks we access have been read before, i.e. the fields first_item_type_or_zero and first_item_id_or_zero in the pbf_block_start struct are populated. Note that this is likely to hit the same few indices in the beginning, thus quickly populating key indices, and reducing the search space dramatically in the first few iterations. If an unpopulated block_start is encountered, proceed with the next stage. Otherwise, return a result as per above rules.
             * 2. Scan the remaining search space linearly. Since this does not access the underlying file, and scans a std::vector, this should be reasonably quick. Whenever a populated block_start is encountered, update begin_search or end_search accordingly. If this reduces the search space to zero or one blocks, return a result as per above rules. Otherwise, proceed with the next stage.
             * 3. At this point, the remaining search space must have length two or more and must contain only unpopulated block_starts. At this point, there is no way to make a good guess: Return the block index in the middle of the search space.
             *
             * @pre begin_search < end_search, i.e. the search space is non-empty.
             * @pre end_search <= this->block_starts().size()
             * @pre The data must be sorted by type first, and object id second
             * @returns The index of a block that seems promising
             * @post begin_search <= end_search
             * @post if begin_search < end_search, then begin_search <= return_value && return_value < end_search
             * @post if begin_search == end_search, then return_value == this->block_starts().size()
             */
            size_t binary_search_object_guess(
                osmium::item_type needle_item_type,
                osmium::object_id_type needle_item_id,
                size_t& begin_search,
                size_t& end_search
            ) const {
                assert(end_search <= m_block_starts.size());

                /* Stage 1: Optimistic binary search
                 * TODO: keep track of whether or not m_block_starts[begin_search] has already been checked
                 * Note that such logic is bug-prone, and unlikely to make any difference. Hence it is not implemented (yet).
                 */
                while (true) {
                    assert(begin_search < end_search);
                    if (begin_search == end_search - 1) {
                        /* Search space has length one. Note that it is possible that begin_search was never modified so far, so we need to check the corresponding block_start first: */
                        if (m_block_starts[begin_search].is_needle_definitely_before(needle_item_type, needle_item_id)) {
                            end_search = begin_search;
                            return m_block_starts.size();
                        }
                        /* Don't care if the block is populated or not, since we cannot reliably tell whether it contains the needle anyway. */
                        return begin_search;
                    }
                    /* Search space has length at least two, so try to halve it: */
                    size_t middle_search = osmium::io::detail::binsearch_middle(begin_search, end_search);
                    const auto& middle_block_start = m_block_starts[middle_search];
                    if (!middle_block_start.is_populated()) {
                        /* Give up, go to stage 2. */
                        break;
                    }
                    if (middle_block_start.is_needle_definitely_before(needle_item_type, needle_item_id)) {
                        /* Exclude the "middle" block: */
                        end_search = middle_search;
                    } else {
                        /* Include the "middle" block: */
                        begin_search = middle_search;
                    }
                }

                /* Stage 2: Linear scan */
                for (size_t middle_search = begin_search; middle_search < end_search; ++middle_search) {
                    const auto& middle_block_start = m_block_starts[middle_search];
                    if (!middle_block_start.is_populated()) {
                        continue;
                    }
                    if (middle_block_start.is_needle_definitely_before(needle_item_type, needle_item_id)) {
                        /* Exclude the "middle" block. Note that this also effectively exits the loop. */
                        end_search = middle_search;
                    } else {
                        /* Include the "middle" block: */
                        begin_search = middle_search;
                    }
                }
                /* At this point, it is possible that the search space contains any number of indices, including just zero or one index. These must be handled separately, to indicate these speecial conditions: */
                if (begin_search == end_search) {
                    return m_block_starts.size();
                }
                if (begin_search == end_search - 1) {
                    return begin_search;
                }

                /* Stage 3: Blindly guess */
                return osmium::io::detail::binsearch_middle(begin_search, end_search);
            }

            /**
             * Execute a binary search for the "needle" OSMObject, assuming that the data are sorted by type first, and ID second.
             *
             * This is a simple high-level function that is easy to use: Either the needle is in the returned buffer, or it is definitely not in the data at all. This comes with a price: Speculatively decompressed buffers cannot be accessed by the caller, or cached in any capacity.
             *
             * - If the returned buffer is invalid, the search conclusively proved that the data definitely do not contain the needle.
             * - If the returned buffer is valid, it may or may not contain the needle. Furthermore, all other blocks *definitely* do not contain it.
             *
             * This modifies the internal state, as it updates the index that backs block_starts().
             *
             * @pre The data must be sorted by type first, and object id second
             * @returns The decoded block
             */
            std::vector<std::unique_ptr<osmium::memory::Buffer>> binary_search_object(
                    const osmium::item_type needle_item_type,
                    const osmium::object_id_type needle_item_id,
                    const osmium::io::read_meta read_metadata
            ) {
                size_t begin_search = 0;
                size_t end_search = m_block_starts.size();
                /* Use binary search and a linear scan on the index to determine a contiguous interval of unpopulated blocks that might contain the needle. Note that the result is discarded intentionally. */
                binary_search_object_guess(needle_item_type, needle_item_id, begin_search, end_search);

                while (end_search - begin_search >= 2) {
                    size_t middle_search = osmium::io::detail::binsearch_middle(begin_search, end_search);
                    assert(!m_block_starts[middle_search].is_populated());
                    std::vector<std::unique_ptr<osmium::memory::Buffer>> buffers = get_parsed_block(middle_search, read_metadata);
                    assert(m_block_starts[middle_search].is_populated());
                    if (m_block_starts[middle_search].is_needle_definitely_before(needle_item_type, needle_item_id)) {
                        end_search = middle_search;
                        continue;
                    }
                    /* At this point, the block *might* contain the needle, or the needle might be in a later block, but definitely doesn't exist before this block.
                     * The obvious approach is to discard 'buffer' and continue the recursive binary search until the search space has only length 0 or 1.
                     * However, note that all the heavy work for the current block has already been done! Exploit that, and search the buffer before continue recursing.
                     * Since buffers are contiguous chunks of memory, this might even be reasonably fast.
                     * TODO: Measure, and perhaps store also the *last* object type and ID in pbf_block_start.
                     * TODO: Measure, and perhaps discard the current block in favor of eager binary search.
                     * (This needs to be done in get_parsed_block.)
                     * As a convenience, only search the last buffer of the block. This means that it cannot always be determined if this block contains the needle.
                     */
                    const osmium::memory::Buffer& last_buffer = *buffers.front();
                    bool saw_less_in_last_buffer = false;
                    for (auto it = last_buffer.begin<osmium::OSMObject>(); it != last_buffer.end<osmium::OSMObject>(); ++it) {
                        int cmp = osmium::io::detail::compare_by_type_then_id(needle_item_type, needle_item_id, it->type(), it->id());
                        if (cmp == 0) {
                            /* Can abort the search here, since accidentally the answer was found.
                             * Note that because the last object ID is not cached, the next call for this exact type and ID  will actually be slower, as it will do a more naive binary search. See the to-do above. */
                            return buffers;
                        }
                        if (cmp < 0) {
                            if (saw_less_in_last_buffer) {
                                /* Can abort the search here, since the needle would have needed to appeared before the current item.
                                 * Note that because the last object ID is not cached, the next call for this exact type and ID  will actually be slower, as it will do a more naive binary search. See the to-do above. */
                                return {};
                            } else {
                                /* Can abort the search here, since the needle is greater-equal to the first object in the first buffer of this block, and smaller than the first object in the last buffer. This means the needle cannot be in any other block. */
                                return buffers;
                            }
                        }
                        saw_less_in_last_buffer = true;
                    }
                    /* The buffer definitely does not contain the needle. */
                    begin_search = middle_search + 1;
                    assert(begin_search <= end_search);
                }
                if (begin_search == end_search) {
                    return {};
                }
                assert(begin_search == end_search - 1);
                return get_parsed_block(begin_search, read_metadata);
            }

        }; // class PbfBlockIndexTable

        class CachedRandomAccessPbf {

            // Each block is between 120 KiB and 8 MiB of data.
            // TODO: This should be more easily configurable; but I have the RAM, so let's use it:
            static const size_t IDEAL_CACHE_SIZE = 2048;

            struct cache_entry {
                size_t borrow_count {0};
                std::vector<std::unique_ptr<osmium::memory::Buffer>> reverse_buffers;
            };

            struct borrow {
                size_t block_id;
                osmium::OSMObject* object;

                bool is_valid() const {
                    return object != nullptr;
                }
                // If the borrow is not valid, then `block_id` is not completely garbage:
                // * "0" for "the needle should have been found, should definitely be in this block, and we have proof that it doesn't exist"
                // * "1" for "the needle wasn't found, but it's still possible that it's in a later block"
                // TODO: Pass this information in a more reasonable way.
                static const size_t NEEDLE_DEFINITELY_MISSING = 0;
                static const size_t NEEDLE_POSSIBLY_LATER = 1;

                // FIXME: RAII reference counting?
            };

            // FIXME: It would be nice to have some kind of feedback from a concurrent linear scan, which could initialize the table "for free".
            PbfBlockIndexTable& m_pbf_table;
            // FIXME: This level of nesting and pointer chasing is unnecessary. All Buffers should live in the same contiguous block of memory.
            std::unordered_map<size_t, cache_entry> m_cache;
            std::minstd_rand m_rng;

            void prune_to_ideal_size(size_t avoid_block_id) {
                if (m_cache.size() < IDEAL_CACHE_SIZE * 3 / 2) {
                    // The effort does not justify the memory savings.
                    return;
                }
                std::vector<size_t> block_ids;
                for (auto const& pair : m_cache) {
                    if (pair.second.borrow_count == 0 && pair.first != avoid_block_id) {
                        block_ids.push_back(pair.first);
                    }
                }
                std::shuffle(block_ids.begin(), block_ids.end(), m_rng); // TODO: Doesn't this move???
                for (size_t block_id : block_ids) {
                    if (m_cache.size() <= IDEAL_CACHE_SIZE) {
                        return;
                    }
                    m_cache.erase(block_id);
                }
            }

            cache_entry& read_without_borrow(size_t block_id) {
                prune_to_ideal_size(block_id);
                cache_entry& entry = m_cache[block_id];
                // Can't prune after here, because we do not hold a borrow.
                if (entry.reverse_buffers.empty()) {
                    assert(entry.borrow_count == 0);
                    entry.reverse_buffers = std::move(m_pbf_table.get_parsed_block(block_id, osmium::io::read_meta::no));
                }
                return entry;
            }

            borrow search_and_borrow_object_in_block(
                    const osmium::item_type needle_item_type,
                    const osmium::object_id_type needle_item_id,
                    size_t block_index,
                    cache_entry& entry
            ) {
                for (auto it = entry.reverse_buffers.rbegin(); it != entry.reverse_buffers.rend(); ++it) {
                    auto& buffer = *it;
                    for (auto it = buffer->begin<osmium::OSMObject>(); it != buffer->end<osmium::OSMObject>(); ++it) {
                        int cmp = osmium::io::detail::compare_by_type_then_id(needle_item_type, needle_item_id, it->type(), it->id());
                        if (cmp == 0) {
                            // Found! Note that we return a pointer into the Buffer itself, which is held by a unique_ptr, and thus unaffected by any rehashing of m_cache, as long as it is not completely removed and destructed. Hence, increase the borrow count (or "reference count") first:
                            entry.borrow_count += 1;
                            return borrow{block_index, &*it};
                        }
                        if (cmp < 0) {
                            // We're past the point where the needle should have been, so the needle does not exist.
                            return borrow{borrow::NEEDLE_DEFINITELY_MISSING, nullptr};
                        }
                    }
                }
                // We never encountered the needle, so the needle does not exist in this block.
                return borrow{borrow::NEEDLE_POSSIBLY_LATER, nullptr};
            }

            // FIXME: Code duplication with binary_search_object
            borrow search_and_borrow_object(
                    const osmium::item_type needle_item_type,
                    const osmium::object_id_type needle_item_id
            ) {
                size_t begin_search = 0;
                size_t end_search = m_pbf_table.block_starts().size();
                m_pbf_table.binary_search_object_guess(needle_item_type, needle_item_id, begin_search, end_search);

                while (end_search - begin_search >= 2) {
                    size_t middle_search = osmium::io::detail::binsearch_middle(begin_search, end_search);
                    auto& middle_block_start = m_pbf_table.block_starts()[middle_search];
                    assert(!middle_block_start.is_populated());
                    cache_entry& entry = read_without_borrow(middle_search);
                    // Note that 'cache_entry' now points to a member of m_cache, so any modification of m_cache could invalidate the reference, or even remove the block entirely.
                    assert(middle_block_start.is_populated());
                    if (middle_block_start.is_needle_definitely_before(needle_item_type, needle_item_id)) {
                        end_search = middle_search;
                        continue;
                    }
                    /* At this point, the block *might* contain the needle, or the needle might be in a later block, but definitely doesn't exist before this block.
                     * The obvious approach is to discard 'buffer' and continue the recursive binary search until the search space has only length 0 or 1.
                     * Note that all the heavy work for the current block has already been done! Exploit that, and search the buffer before continue recursing.
                     * Since buffers are contiguous chunks of memory, this might even be reasonably fast.
                     * TODO: Measure, and perhaps store also the *last* object type and ID in pbf_block_start.
                     * TODO: Measure, and perhaps discard the current block in favor of eager binary search.
                     * (This needs to be done in get_parsed_block.)
                     * As a convenience, only search the last buffer of the block. This means that it cannot always be determined if this block contains the needle.
                     */
                    auto item_borrow = search_and_borrow_object_in_block(needle_item_type, needle_item_id, middle_search, entry);
                    if (item_borrow.is_valid()) {
                        // Found! Note that we return a pointer into the Buffer itself, which is held by a unique_ptr,
                        // and thus unaffected by any rehashing of m_cache, as long as it is not completely removed and
                        // destructed. Hence, increase the borrow count (or "reference count") first:
                        return item_borrow;
                    }
                    assert(item_borrow.block_id == borrow::NEEDLE_DEFINITELY_MISSING || item_borrow.block_id == borrow::NEEDLE_POSSIBLY_LATER);
                    if (item_borrow.block_id == borrow::NEEDLE_DEFINITELY_MISSING) {
                        /* The buffer doesn't contain the needle, and this is the only block where it could have been. */
                        return borrow{0, nullptr};
                    }
                    assert(middle_search > begin_search);
                    begin_search = middle_search + 1;
                    assert(begin_search <= end_search);
                }
                if (begin_search == end_search) {
                    return borrow{0, nullptr};
                }
                assert(begin_search == end_search - 1);
                auto& entry = read_without_borrow(begin_search);
                // Note that 'cache_entry' now points to a member of m_cache, so any modification of m_cache could invalidate the reference, or even remove the block entirely.
                auto item_borrow = search_and_borrow_object_in_block(needle_item_type, needle_item_id, begin_search, entry);
                if (item_borrow.is_valid()) {
                    // Found! Note that we return a pointer into the Buffer itself, which is held by a unique_ptr,
                    // and thus unaffected by any rehashing of m_cache, as long as it is not completely removed and
                    // destructed. Hence, increase the borrow count (or "reference count") first:
                    return item_borrow;
                }
                assert(item_borrow.block_id == borrow::NEEDLE_DEFINITELY_MISSING || item_borrow.block_id == borrow::NEEDLE_POSSIBLY_LATER);
                // Erase the location information, as the caller should not rely on it.
                return borrow{0, nullptr};
            }

            void return_borrow(borrow const& item_borrow) {
                assert(item_borrow.is_valid());
                auto& entry = m_cache[item_borrow.block_id];
                assert(entry.borrow_count > 0);
                entry.borrow_count -= 1;
            }

        public:
            explicit CachedRandomAccessPbf(PbfBlockIndexTable& table) :
                m_pbf_table(table),
                m_rng(static_cast<uint_fast32_t>(std::random_device()()))
            {
            }

            osmium::OSMObject& first_in_block(size_t block_index) {
                auto const& reverse_buffers = read_without_borrow(block_index).reverse_buffers;
                return *reverse_buffers.back()->begin<osmium::OSMObject>();
            }

            // TCallback must be of type "void fn(osmium::OSMObject const& obj)", or some other return type, or an object with "operator()(osmium::OSMObject const&)".
            template<typename TCallback>
            bool visit_object(
                    const osmium::item_type needle_item_type,
                    const osmium::object_id_type needle_item_id,
                    TCallback callback
            ) {
                borrow item_borrow = search_and_borrow_object(needle_item_type, needle_item_id);
                if (!item_borrow.is_valid()) {
                    return false;
                }
                callback(*item_borrow.object);
                return_borrow(item_borrow);
                return true;
            }

            // TCallback must be of type "void fn(osmium::Node const& node)", or some other return type, or an object with "operator()(osmium::Node const&)".
            template<typename TCallback>
            bool visit_node(
                    const osmium::object_id_type needle_item_id,
                    TCallback callback
            ) {
                return visit_object(osmium::item_type::node, needle_item_id, [&](osmium::OSMObject const& obj){ callback(static_cast<osmium::Node const&>(obj)); });
            }

            // TCallback must be of type "void fn(osmium::Way const& way)", or some other return type, or an object with "operator()(osmium::Way const&)".
            template<typename TCallback>
            bool visit_way(
                    const osmium::object_id_type needle_item_id,
                    TCallback callback
            ) {
                return visit_object(osmium::item_type::way, needle_item_id, [&](osmium::OSMObject const& obj){ callback(static_cast<osmium::Way const&>(obj)); });
            }

            // TCallback must be of type "void fn(osmium::Relation const& relation)", or some other return type, or an object with "operator()(osmium::Relation const&)".
            template<typename TCallback>
            bool visit_relation(
                    const osmium::object_id_type needle_item_id,
                    TCallback callback
            ) {
                return visit_object(osmium::item_type::relation, needle_item_id, [&](osmium::OSMObject const& obj){ callback(static_cast<osmium::Relation const&>(obj)); });
            }

        }; // class CachedRandomAccessPbf

    } // namespace io

} // namespace osmium

#endif // OSMIUM_IO_PBF_INPUT_RANDOMACCESS_HPP
