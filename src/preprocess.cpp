#include <stdio.h>
#include <ctype.h>
#include <stdint.h>
#include <stdarg.h>
#include <fstream>
#include <stdexcept>
#include <map>
#include <list>
#include <string>
#include <algorithm>

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

using namespace std;
namespace io = boost::iostreams;

// An active parsing frame
struct parse_frame {
	bool redirect; // If true, content is the redirect target
	xmlChar *title, *content;

	parse_frame() : redirect(false), title(NULL), content(NULL) {
	}
};

struct result_target {
	FILE *f_ids, *f_names, *f_links;
	strtree idTree;
	map<uint32_t, uint32_t> offsetMap;
	map<string, list<uint32_t> > unresolved;
	map<uint32_t, list<uint32_t> > links;
	uint32_t currentID, n_unresolved;
};

void tolower(char* s) {
	for(;*s != '\0';s++) *s = tolower(*s);
}

void processFrame(parse_frame& frame, result_target& out) {
	using namespace boost;
	uint32_t ident = ++out.currentID;
	if(frame.content == NULL || frame.title == NULL) {
		printf("\nWarning: Found page with null frame/content\n");
		fflush(stdout);
		return;
	}
	printf("\rProcessing pages [%8d|%8d]: %120s", out.currentID,
			out.n_unresolved, frame.title);
	string tstr((const char*)frame.title);

	// Write the name
	out.offsetMap[ident] = ftell(out.f_names);
	fwrite(frame.title, 1, xmlStrlen(frame.title), out.f_names);

	// Save the title in the ID buffer
	out.idTree.set(frame.title, ident);

	// Resolve links
	map<string, list<uint32_t> >::iterator itr;
	itr = out.unresolved.find(tstr);
	if(itr != out.unresolved.end()) {
		list<uint32_t>& l = itr->second;
		for(list<uint32_t>::iterator i=l.begin();i != l.end();i++) {
			out.links[*i].push_back(ident);
			out.n_unresolved--;
		}
		out.unresolved.erase(itr);
	}

	// Store unresolved links
	string cstr((const char*)frame.content);
	static const regex linkRE("\\[\\[([^|\\]]+)(\\|[^\\]]+)?\\]\\]",
			regex::perl);
	sregex_iterator iter(cstr.begin(), cstr.end(), linkRE);
	sregex_iterator endIter;
	list<uint32_t> resolved;
	for(;iter != endIter;iter++) {
		sregex_iterator::value_type m = *iter;
		// Get the name
		const string link = m.str(1);

		// Check if it's already resolved
		if(out.idTree.has(BAD_CAST link.c_str())) {
			resolved.push_back(out.idTree.get(BAD_CAST link.c_str()));
		} else {
			itr = out.unresolved.find(link);
			if(itr == out.unresolved.end()) {
				list<uint32_t> nlist;
				nlist.push_back(ident);
				out.unresolved[link] = nlist;
				out.n_unresolved++;
			} else {
				itr->second.push_back(ident);
				out.n_unresolved++;
			}
		}
	}
	out.links[ident] = resolved;
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
	target.n_unresolved = 0;
	int ret;
	while((ret = xmlTextReaderRead(reader)) == 1 && target.currentID < 60000) {
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

	xmlCleanupParser();
	return 0;
}
