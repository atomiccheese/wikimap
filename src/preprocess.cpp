#include <stdio.h>
#include <ctype.h>
#include <stdint.h>
#include <stdarg.h>
#include <fstream>
#include <stdexcept>
#include <map>
#include <list>
#include <vector>
#include <string>
#include <algorithm>
#include <locale>

#include <libxml/xmlreader.h>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/operations.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/regex.hpp>

#include "rbt.hpp"
#include "bytes.hpp"
#include "strtree.hpp"
#include "patricia.hpp"

using namespace std;
namespace io = boost::iostreams;

// An active parsing frame
struct parse_frame {
	bool redirect; // If true, content is the redirect target
	xmlChar *title, *content;

	parse_frame() : redirect(false), title(NULL), content(NULL) {
	}
};

// Link structure during streaming processing
struct streaming_link {
	uint32_t target;
	bool redirect;
};

struct result_target {
	FILE *f_ids, *f_names, *f_links;
	ui32patricia idTree;
	patricia_trie<vector<streaming_link>*> relocate;
	map<uint32_t, list<uint32_t> > links;
	map<uint32_t, size_t> offsetMap; // File offsets of name info
	uint32_t currentID;
};

void tolower(char* s) {
	for(;*s != '\0';s++) *s = tolower(*s);
}

void processFrame(parse_frame& frame, result_target& out) {
	using namespace boost;
	uint32_t ident = ++out.currentID;
	if(frame.content == NULL || frame.title == NULL) {
		if(frame.title != NULL && frame.content == NULL)
			printf("\nWarning: page '%s' has null content\n", frame.title);
		else if(frame.title == NULL && frame.content != NULL)
			printf("\nWarning: page has null title\n");
		else
			printf("\nWarning: page has null frame and content\n");
		fflush(stdout);
		return;
	}
	if(out.currentID % 64 == 0)
		printf("\rProcessing pages [%8d]: %120s", out.currentID, frame.title);
	string tstr((const char*)frame.title);

	// Write the name
	out.offsetMap[ident] = ftell(out.f_names);
	fwrite(frame.title, 1, xmlStrlen(frame.title), out.f_names);
	char null = 0;
	fwrite(&null, 1, 1, out.f_names);

	// Save the title in the ID buffer
	out.idTree.insert((const char*)(frame.title), ident);

	// Store links
	string cstr((const char*)frame.content);
	static const regex linkRE("\\[\\[([^|\\]]+)(\\|[^\\]]+)?\\]\\]",
			regex::perl);
	sregex_iterator iter(cstr.begin(), cstr.end(), linkRE);
	sregex_iterator endIter;
	for(;iter != endIter;iter++) {
		sregex_iterator::value_type m = *iter;
		// Get the target
		const string link = m.str(1);

		vector<streaming_link>* linkList = out.relocate.lookup(link, NULL);
		if(linkList == NULL) {
			linkList = new vector<streaming_link>();
			out.relocate.insert(link, linkList);
		}
		streaming_link l;
		l.target = ident;
		l.redirect = frame.redirect;
		linkList->push_back(l);
	}
}

void storePatricia(ui32patricia::node_type* node,
		map<ui32patricia::node_type*, size_t>& relocation, FILE* out) {
	// Update relocation information
	size_t begin = ftell(out);
	map<ui32patricia::node_type*, size_t>::iterator itr = relocation.find(node);
	if(itr != relocation.end()) {
		fseek(out, itr->second, SEEK_SET);
		writeInt32(begin, out);
		fseek(out, begin, SEEK_SET);
		relocation.erase(itr);
	}
	printf("\rWriting relocation data (%10d)", relocation.size());

	// Write the value, if present
	char hasValue = node->hasValue ? 1 : 0;
	fwrite(&hasValue, 1, 1, out);
	if(node->hasValue) writeInt32(node->value, out);

	// Count the edge list length and store it
	uint16_t numEdges = 0;
	for(ui32patricia::edgelist e=node->edges;e != NULL;e=e->next) numEdges++;

	writeInt16(numEdges, out);

	// Write placeholders for each subnode
	size_t phBase = ftell(out);
	for(ui32patricia::edgelist e=node->edges;e != NULL;e=e->next) {
		// Write the name
		writeInt16(e->first.length(), out);
		fwrite(e->first.c_str(), 1, e->first.length(), out);

		// And the offset
		relocation.insert(make_pair(e->second, ftell(out)));
		writeInt32(0, out);
	}

	// Write subnodes
	for(ui32patricia::edgelist e=node->edges;e != NULL;e=e->next)
		storePatricia(e->second, relocation, out);
}

int boost_stream_read_callback(void* ctx, char* buf, int len) {
	io::filtering_streambuf<io::input>* p =
		(io::filtering_streambuf<io::input>*)ctx;
	return io::read(*p, buf, len);
}

int boost_stream_close_callback(void* ctx) {
	io::filtering_streambuf<io::input>* p =
		(io::filtering_streambuf<io::input>*)ctx;
	io::close(*p);
	return 0;
}

void fail(int n, const char* msg, ...) {
	va_list v;
	va_start(v, msg);
	vfprintf(stderr, msg, v);
	va_end(v);
	exit(n);
}

int main(int argc, char **argv) {
	if(argc != 2)
		fail(1, "Usage: %s [compressed database file]\n", argv[0]);
	LIBXML_TEST_VERSION

	// Open the file and start parsing XML
	ifstream inStream(argv[1], ios_base::in | ios_base::binary);
	io::filtering_streambuf<io::input> file;
	file.push(io::bzip2_decompressor());
	file.push(inStream);

	xmlTextReaderPtr reader = xmlReaderForIO(
			boost_stream_read_callback, boost_stream_close_callback,
			&file, "", NULL, 0);
	if(reader == NULL)
		fail(1, "Cannot create XML reader\n");

	// Open output files
	result_target target;
	target.f_ids = fopen("ids.bin", "wb");
	target.f_names = fopen("names.bin", "wb");
	target.f_links = fopen("links.bin", "wb");
	if(target.f_ids == NULL)
		fail(2, "Cannot open ids.bin\n");
	if(target.f_names == NULL)
		fail(2, "Cannot open names.bin\n");
	if(target.f_links == NULL)
		fail(2, "Cannot open links.bin\n");

	// Process the file, and build the necessary mappings. While elements are
	// processed, add complete pages to the name->id map, and write the names
	// out to a file, saving the associated positions.
	parse_frame active_frame;
	bool parsing_frame = false;

	target.currentID = 0;
	int ret;
	while((ret = xmlTextReaderRead(reader)) == 1) {
		// Process a node and print it
		const xmlChar* localname = xmlTextReaderConstLocalName(reader);
		int nodeType = xmlTextReaderNodeType(reader);
		bool is_end = nodeType == XML_READER_TYPE_END_ELEMENT,
			is_begin = nodeType == XML_READER_TYPE_ELEMENT;

		if(xmlStrEqual(localname, BAD_CAST "page")) {
			if(is_end && parsing_frame) {
				// Check if we have an active frame, and complete it if so
				processFrame(active_frame, target);
				parsing_frame = false;
			} else if(is_begin) {
				// Clean up the active frame for the new page
				parsing_frame = true;
				active_frame.redirect = false;
				if(active_frame.title != NULL) {
					xmlFree(active_frame.title);
					active_frame.title = NULL;
				}
				if(active_frame.content != NULL) {
					xmlFree(active_frame.content);
					active_frame.content = NULL;
				}
			}
		} else if(xmlStrEqual(localname, BAD_CAST "title")) { // Title
			if(is_end) continue;
			active_frame.title = xmlTextReaderReadString(reader);
		} else if(xmlStrEqual(localname, BAD_CAST "redirect")) { // Redirect
			if(is_end) continue;
			active_frame.redirect = true;
			active_frame.content = xmlTextReaderGetAttributeNo(reader, 0);
		} else if(xmlStrEqual(localname, BAD_CAST "text")) { // Text
			if(is_end || active_frame.redirect) continue;
			if(active_frame.content != NULL) continue;
			active_frame.content = xmlTextReaderReadString(reader);
		}
	}
	printf("\nParsing complete\n");

	// Now that link and name mappings are done, postprocess the link maps into
	// their final form
	map<ui32patricia::node_type*, size_t> relocationTable;
	storePatricia(target.idTree.root, relocationTable, target.f_ids);

	xmlCleanupParser();
	return 0;
}
