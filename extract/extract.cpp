// #include <algorithm> // sort
#include <cassert>
#include <cstdio>
#include <cstring> // strcmp
// #include <functional> // greater
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <osmium/io/pbf_input.hpp>
#include <osmium/io/pbf_input_randomaccess.hpp>
#include <osmium/io/reader_with_progress_bar.hpp>
#include <osmium/handler.hpp>
#include <osmium/visitor.hpp>

static char const* TAGS_THAT_OFTEN_CONAIN_URLS[] = {
    // This list is highly debatable. Please feel free to suggest improvements.
    "brand:website",
    "contact:takeaway",
    "contact:url",
    "contact:webcam",
    "contact:website",
    "destination:url",
    "facebook",
    "fee:source",
    "flickr",
    "heritage:website",
    "image:0",
    "image2",
    "image:streetsign",
    "inscription:url",
    "instagram",
    "internet",
    "market:flea_market:opening_hours:url",
    "memorial:website",
    "menu:url",
    "name:etymology:website",
    "network:website",
    "note:url",
    "opening_hours:url",
    "operator:website",
    "picture",
    "post_office:website",
    "rail_trail:website",
    "railway:source",
    "source:1",
    "source:2",
    "source_2",
    "source2",
    "source:3",
    "source:heritage",
    "source:image",
    "source:office",
    "source:old_ref",
    "source:operator",
    "source:payment:contactless",
    "source:phone",
    "source:railway:radio",
    "source:railway:speed_limit_distant:speed",
    "source:railway:speed_limit:speed",
    "source:ref",
    "source_url",
    "source:url",
    "source:website",
    "symbol:url",
    "url",
    "url:official",
    "url:timetable",
    "video_2",
    "webcam",
    "website",
    "website_1",
    "website2",
    "website:booking",
    "website:DDB",
    "website:en",
    "website:LfDH",
    "website:menu",
    "website:orders",
    "website:regulation",
    "website:stock",
    "website:VDMT",
    "xmas:url"
};

// Some Relations are deeply-nested, or mostly lie outside the extracted territory, which causes terrible performance.
// Hardcode these relations to skip their cost entirely.
struct HardcodedLocation {
    osmium::object_id_type id;
    double x;
    double y;
};
static const HardcodedLocation HARDCODED_RELATION_LOCATIONS[] = {
    // SEARCH: ^WARNING: Very expensive resolution: r(\d+) took (\d+) backrefs\?! Consider hardcoding to ([0-9.]+), ([0-9.]+)  $
    // REPLACE:     {\1, \3, \4}, // \2 backrefs
    {20828, 9.424950, 54.832655}, // 1827 backrefs
    {61491, 9.359337, 54.819907}, // 1149 backrefs
    {181093, 14.222385, 50.859423}, // 1455 backrefs
    {299546, 9.361681, 54.816516}, // 1016 backrefs
    {912994, 13.786577, 48.558202}, // 1732 backrefs
    {2521076, 6.224311, 51.359232}, // 1334 backrefs
    {2689634, 9.424950, 54.832655}, // 1309 backrefs
    {3088664, 14.214609, 53.877682}, // 1314 backrefs
    {7190393, 7.955247, 47.540841}, // >1000 backrefs
    {7190394, 7.922629, 47.544431}, // >1000 backrefs
    {9244345, 7.922629, 47.544431}, // 2391 backrefs
    {9351570, 12.952523, 47.768681}, // 1740 backrefs
    {9351571, 12.179740, 47.599290}, // 1302 backrefs
    {9351572, 12.952523, 47.768681}, // 1741 backrefs
    {11305708, 6.224311, 51.359232}, // 1331 backrefs
    {13971563, 7.651894, 49.044413} // 1636 backrefs
    // In total, this small table prevents 6.4% of all backrefs!
};

static bool looks_like_url(char const* const str) {
    return 0 == strncmp(str, "http", 4);
}

class Occurrence {
public:
    osmium::item_type item_type;
    osmium::object_id_type item_id;
    std::string tag_key;
    double lon;
    double lat;
};

class ResultWriter {

#define IO_EXPECT(bool_stmt)                                 \
    do {                                                     \
        bool _io_expect_success = (bool_stmt);               \
        if (!_io_expect_success) {                           \
            int saved_errno = errno;                         \
            fprintf(stderr, "IO FAILED: %s\n", #bool_stmt);  \
            fprintf(stderr, "    errno: %d\n", saved_errno); \
            assert(0);                                       \
            exit(1);                                         \
        }                                                    \
    } while(0)

    FILE* m_fp;
    bool m_any_findings {false};

public:
    explicit ResultWriter(char const* filename)
        : m_fp(fopen(filename, "w"))
    {
        IO_EXPECT(m_fp != nullptr && "cannot open output for writing");
        IO_EXPECT(fprintf(m_fp, "{\"v\": 2, \"type\": \"monitor-osm-domains extraction results\", \"findings\": [") > 0);
    }
    ~ResultWriter()
    {
        assert(m_fp != nullptr);
        IO_EXPECT(fprintf(m_fp, "\n]}") > 0);
        IO_EXPECT(fflush(m_fp) == 0);
        IO_EXPECT(fclose(m_fp) == 0);
    }

    void write_record(std::string const& url, std::vector<Occurrence> const& occurrences) {
        // I know that these calls can probably be glued together or converted to fputs.
        // However, this part of the program is fast enough, the compiler probably does this already,
        // and I strongly prefer readability/verifiability in this case.
        if (m_any_findings) {
            IO_EXPECT(fprintf(m_fp, ",") > 0);
        } else {
            m_any_findings = true;
        }
        IO_EXPECT(fprintf(m_fp, "\n") > 0);
        IO_EXPECT(fprintf(m_fp, " {") > 0);

        IO_EXPECT(fprintf(m_fp, "\"url\": \"") > 0);
        write_string(url);
        IO_EXPECT(fprintf(m_fp, "\"") > 0);

        IO_EXPECT(fprintf(m_fp, ", ") > 0);
        IO_EXPECT(fprintf(m_fp, "\"occ\": [") > 0);
        write_occurrences(occurrences);
        IO_EXPECT(fprintf(m_fp, "\n  ]}") > 0);
    }

private:
    void write_occurrences(std::vector<Occurrence> const& occurrences) {
        bool first_occurrence = true;
        for (auto const& occurrence : occurrences) {
            if (first_occurrence) {
                first_occurrence = false;
            } else {
                IO_EXPECT(fprintf(m_fp, ",") > 0);
            }
            IO_EXPECT(fprintf(m_fp, "\n  ") > 0);
            write_occurrence(occurrence);
        }
    }

    void write_occurrence(Occurrence const& occurrence) {
        IO_EXPECT(fprintf(m_fp, "{") > 0);

        IO_EXPECT(fprintf(m_fp, "\"t\": \"%c\"", osmium::item_type_to_char(occurrence.item_type)) > 0);
        IO_EXPECT(fprintf(m_fp, ", ") > 0);
        IO_EXPECT(fprintf(m_fp, "\"id\": %lu", occurrence.item_id) > 0);
        IO_EXPECT(fprintf(m_fp, ", ") > 0);
        IO_EXPECT(fprintf(m_fp, "\"k\": \"") > 0);
        write_string(occurrence.tag_key);
        IO_EXPECT(fprintf(m_fp, "\"") > 0);
        IO_EXPECT(fprintf(m_fp, ", ") > 0);
        IO_EXPECT(fprintf(m_fp, "\"x\": %f", occurrence.lon) > 0);
        IO_EXPECT(fprintf(m_fp, ", ") > 0);
        IO_EXPECT(fprintf(m_fp, "\"y\": %f", occurrence.lat) > 0);

        IO_EXPECT(fprintf(m_fp, "}") > 0);
    }

    void write_string(std::string const& str) {
        // Blatantly stolen from serenity/AK/StringBuilder.cpp: StringBuilder::try_append_escaped_for_json
        for (char c : str) {
            switch (c) {
            case '\b':
                IO_EXPECT(fputs("\\b", m_fp) > 0);
                break;
            case '\n':
                IO_EXPECT(fputs("\\n", m_fp) > 0);
                break;
            case '\t':
                IO_EXPECT(fputs("\\t", m_fp) > 0);
                break;
            case '\"':
                IO_EXPECT(fputs("\\\"", m_fp) > 0);
                break;
            case '\\':
                IO_EXPECT(fputs("\\\\", m_fp) > 0);
                break;
            default:
                // Note that "c < 0" happens with UTF-8 multi-byte sequences.
                if (c >= 0 && c <= 0x1f)
                    IO_EXPECT(fprintf(m_fp, "\\u%04x", c) > 0);
                else
                    IO_EXPECT(fputc(c, m_fp) > 0);
            }
        }
    }

#undef IO_EXPECT

};

class FindUrlHandler : public osmium::handler::Handler {
    std::unordered_set<std::string> m_cached_url_tag_keys {};
    std::unordered_map<std::string, std::vector<Occurrence>> m_records {};
    osmium::io::CachedRandomAccessPbf& m_resolver;
    size_t m_num_occurrences {0};
    size_t m_num_backrefs {0};

    osmium::item_type m_most_expensive_type {osmium::item_type::undefined};
    osmium::object_id_type m_most_expensive_id {0};
    size_t m_most_expensive_backrefs {0};

public:
    FindUrlHandler(osmium::io::CachedRandomAccessPbf& resolver) :
        m_resolver(resolver)
    {
        for (char const* const tag_key : TAGS_THAT_OFTEN_CONAIN_URLS) {
            m_cached_url_tag_keys.insert(tag_key);
        }
        // We expect to find nearly a million URLs. Skip a few rehashing steps by make the hashtable large from the start:
        m_records.reserve(100'000);
    }

    void osm_object(osmium::OSMObject const& obj)
    {
        for (auto const& tag : obj.tags()) {
            // Because checking the first four characters is probably faster, and also rules out most wrong tags anyway, do that first:
            if (!looks_like_url(tag.value())) {
                // Doesn't start with "http".
                continue;
            }
            std::string key = tag.key();
            if (m_cached_url_tag_keys.find(key) == m_cached_url_tag_keys.end()) {
                // Known-bad tag. This means the URL is likely to be something that we don't want to check anyway, like a facebook page.
                continue;
            }
            size_t backrefs_before = m_num_backrefs;
            auto location = resolve_object(obj);
            size_t backrefs_this = m_num_backrefs - backrefs_before;
            if (backrefs_this > m_most_expensive_backrefs) {
                m_most_expensive_backrefs = backrefs_this;
                m_most_expensive_id = obj.id();
                m_most_expensive_type = obj.type();
            }
            if (!location.valid()) {
                printf("\nWARNING: Cannot resolve object %c%lu to any location?!\n\n", osmium::item_type_to_char(obj.type()), obj.id());
                location = osmium::Location(10.0, 50.0);
            }
            if (backrefs_this > 1000) {
                printf(
                    "WARNING: Very expensive resolution: %c%lu took %lu backrefs?! Consider hardcoding to %f, %f  \n",
                    osmium::item_type_to_char(obj.type()), obj.id(), backrefs_this,
                    location.lon(), location.lat());
            }
            auto result = m_records.insert({ tag.value(), {} });
            auto it = result.first;
            it->second.push_back(Occurrence {
                obj.type(),
                obj.id(),
                move(key),
                location.lon(),
                location.lat()
            });
            m_num_occurrences += 1;
        }
    }

    void print_stats() {
        printf("Found %lu unique URLs in %lu values, executed %lu back-references.\n", m_records.size(), m_num_occurrences, m_num_backrefs);
        printf("Most expensive occurrence was %c%lu with %lu backrefs.\n", osmium::item_type_to_char(m_most_expensive_type), m_most_expensive_id, m_most_expensive_backrefs);
    }

    std::unordered_map<std::string, std::vector<Occurrence>> const& records() {
        return m_records;
    }

private:
    osmium::Location resolve_relation(osmium::Relation const& relation) {
        for (auto const& hardcoded_location : HARDCODED_RELATION_LOCATIONS) {
            if (relation.id() == hardcoded_location.id) {
                return osmium::Location(hardcoded_location.x, hardcoded_location.y);
            }
        }
        static const osmium::item_type RECURSION_TYPE_ORDER[] = {
            // Nodes would immediately yield a location.
            osmium::item_type::node,
            // Way are still quick, but require a small detour.
            osmium::item_type::way,
            // Relations have at least one more indirection, possibly multiple.
            // Use them only if no other path to get any location exists:
            osmium::item_type::relation
        };
        for (auto on_type : RECURSION_TYPE_ORDER) {
            for (auto const& memberref : relation.members()) {
                if (memberref.type() != on_type) {
                    // Consider this member later / already considered this member.
                    continue;
                }
                osmium::Location loc;
                m_num_backrefs += 1;
                m_resolver.visit_object(
                    memberref.type(),
                    memberref.ref(),
                    [&](osmium::OSMObject const& object){
                        loc = resolve_object(object);
                    }
                );
                if (loc)
                    return loc;
            }
        }
        return osmium::Location();
    }

    osmium::Location resolve_way(osmium::Way const& way) {
        for (auto const& noderef : way.nodes()) {
            osmium::Location loc;
            m_num_backrefs += 1;
            m_resolver.visit_node(
                noderef.ref(),
                [&](osmium::Node const& node){
                    loc = node.location();
                }
            );
            if (loc)
                return loc;
        }
        return osmium::Location();
    }

    osmium::Location resolve_object(osmium::OSMObject const& object) {
        switch (object.type()) {
        case osmium::item_type::node:
            return static_cast<osmium::Node const&>(object).location();
        case osmium::item_type::way:
            return resolve_way(static_cast<osmium::Way const&>(object));
        case osmium::item_type::relation:
            return resolve_relation(static_cast<osmium::Relation const&>(object));
        default:
            printf("WARNING: Object %c%lu has weird item type %u?!\n", osmium::item_type_to_char(object.type()), object.id(), static_cast<uint16_t>(object.type()));
            return osmium::Location();
        }
    }
};

int main(int argc, char const** argv) {
    if (argc != 3) {
        fprintf(stderr, "USAGE: %s /path/to/input/region-latest.osm.pbf /path/to/output/raw.monosmdom.json\n", argv[0]);
        return 2;
    }
    char const* input_filename = argv[1];
    char const* output_filename = argv[2];

    printf("=== WARNING: This program eats up 6 GiB of RAM. PRESS CTRL-C NOW if this is a problem. ===\n");
    // Fail early: Try to open the output for writing.
    printf("Opening %s for writing …\n", output_filename);
    ResultWriter writer {output_filename};

    printf("Preparing random access index for %s …\n", input_filename);
    osmium::io::PbfBlockIndexTable table {input_filename};
    osmium::io::CachedRandomAccessPbf resolver {table};
    FindUrlHandler find_handler {resolver};
    {
        printf("Linear scan of %s …\n", input_filename);
        osmium::io::ReaderWithProgressBar reader{true, input_filename, osmium::osm_entity_bits::all};
        osmium::apply(reader, find_handler);
        reader.close();
    }

    find_handler.print_stats();
    printf("Writing to %s …\n", output_filename);
    for (auto const& record : find_handler.records()) {
        writer.write_record(record.first, record.second);
    }

    printf("All done!\n");
    return 0;
}
