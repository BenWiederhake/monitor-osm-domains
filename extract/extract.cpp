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

static bool looks_like_url(char const* const str) {
    return 0 == strncmp(str, "http", 4);
}

class Occurrence {
public:
    osmium::item_type item_type;
    osmium::object_id_type item_id;
    std::string tag_key;
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
        IO_EXPECT(fprintf(m_fp, "{\"v\": 1, \"type\": \"monitor-osm-domains extraction results\", \"findings\": [") > 0);
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
    size_t m_num_occurrences {0};

public:
    FindUrlHandler()
    {
        for (char const* const tag_key : TAGS_THAT_OFTEN_CONAIN_URLS) {
            m_cached_url_tag_keys.insert(tag_key);
        }
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
            auto result = m_records.insert({ tag.value(), {} });
            auto it = result.first;
            it->second.push_back(Occurrence {
                obj.type(),
                obj.id(),
                move(key),
            });
            m_num_occurrences += 1;
        }
    }

    size_t num_urls() {
        return m_records.size();
    }

    size_t num_occurrences() {
        return m_num_occurrences;
    }

    std::unordered_map<std::string, std::vector<Occurrence>> const& records()
    {
        return m_records;
    }
};

int main(int argc, char const** argv) {
    if (argc != 3) {
        fprintf(stderr, "USAGE: %s /path/to/input/region-latest.osm.pbf /path/to/output/all.monosmdom.json\n", argv[0]);
        return 2;
    }
    char const* input_filename = argv[1];
    char const* output_filename = argv[2];

    // Fail early: Try to open the output for writing.
    printf("Opening %s for writing …\n", output_filename);
    ResultWriter writer {output_filename};

    printf("Linear scan of %s …\n", input_filename);
    FindUrlHandler find_handler;
    {
        osmium::io::ReaderWithProgressBar reader{true, input_filename, osmium::osm_entity_bits::all};
        osmium::apply(reader, find_handler);
        reader.close();
    }

    printf("Found %lu unique URLs in %lu values, writing to %s …\n", find_handler.num_urls(), find_handler.num_occurrences(), output_filename);
    for (auto const& record : find_handler.records()) {
        writer.write_record(record.first, record.second);
    }

    printf("All done!\n");
    return 0;
}
