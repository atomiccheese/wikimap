#include "linklist.hpp"
#include "bytes.hpp"
#include "rbt.hpp"
#include <utility>
#include <vector>
#include <string>
#include <stdint.h>

#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <cctype>

#include <list>
#include <map>
#include <algorithm>

#include <boost/regex.hpp>

#include <libxml/xmlstring.h>
#include <libxml/parser.h>

using namespace boost;
using namespace std;

/* Reads: WikiMedia official database dumps
 * Emits:
 *	ids.bin		- Serialized binary tree mapping page names to ID numbers
 *	names.bin	- Vector of page IDs mapping to names
 *	contents.bin	- Maps pages to their content text using a skip list
 *	links.bin	- Maps page IDs to the textual links they contain
 */
 
//#define STACK_DEBUG(msg, ...) fprintf(stdout, msg, ##__VA_ARGS__)
#define STACK_DEBUG(msg, ...)

enum StackElementType {
	SE_STRING,	// Stored in 'str'
	SE_TITLE,	// Stored in 'str'
	SE_REDIRECT,	// Stored in 'str'
	SE_INTEGER,	// Stored in 'integer'
	SE_PAGE		// Stored in 'object'
};

enum StackInstruction {
	SI_PAGE,		// Wait for a page tag
	SI_GET_PAGE_ELEM,	// Accept any page content tag
	SI_GET_REV_CONTENT,	// Wait for a text tag
	SI_WAIT_CLOSE,		// Wait for a close-tag then finish the insn
	SI_READ_STR,		// Read the next complete content as a string
	SI_READ_INT,		// Read the next complete content as an int
	SI_CONSTRUCT_PAGE,	// Construct and push a page
	SI_FINALIZE_ONE,	// Pop a Page and push it into the output list
	SI_CONSTRUCT_TITLE	// Convert an SE_STRING to an SE_TITLE
};

struct Link {
	bool resolved;
	union {
		uint32_t ident;
		string* target;
	};
};
typedef vector<Link> LinkList;
struct Page {
	string title;
	int wikiID;
	bool isRedirect;
	bool fullyResolved;
	string target;
	LinkList links;
};

// Writes an 8-level skip list of content pages
class ContentWriter {
public:
	ContentWriter(const char* fname) {
		fptr = fopen(fname, "wb");
	}

	~ContentWriter() {
		fclose(fptr);
	}

	void addEntry(string content) {
		// Get our address
		written++;
		uint64_t addr = ftell(fptr);

		// Check the link queue
		if(linkingQueue.count(written) > 0) {
			pair<multimap<uint32_t,size_t>::iterator,multimap<uint32_t,size_t>::iterator> itrs = linkingQueue.equal_range(written);
		}

		// Write the content
	}

private:
	void seekWrite(size_t pos, uint64_t value, bool restore=true) {
		size_t oldp = ftell(fptr);
		fseek(fptr, pos, SEEK_SET);
		value = swap64(value);
		fwrite(&value, sizeof(value), 1, fptr);
		if(restore) fseek(fptr, oldp, SEEK_SET);
	}

	FILE* fptr;
	uint32_t written;
	multimap<uint32_t, size_t> linkingQueue; // Maps written values to the addresses at which to write the coords
};

struct parsingStackElement {
	StackElementType type;
	string str;
	union {
		int64_t integer;
		void* object;
	};
};

struct parsingState {
	parsingState() {
		id=0;
		insns.push_back(SI_PAGE);
		tagContent = "";
		nParsed = 0;
		err = false;
	}

	int id; // Indentation level

	// Parsing state
	string tagContent;
	list<parsingStackElement> stack;
	list<StackInstruction> insns;
	bool err;
	string errMsg;

	// Output queue
	map<string, uint32_t> namesIDs;
	vector<vector<string> > idsLinks;
	uint32_t nParsed;
};

void printStack(parsingState* st) {
	printf("-------- INSTRUCTION STACK --------\n");
	for(list<StackInstruction>::iterator i=st->insns.begin();i != st->insns.end();i++) {
		const char* s;
		switch(*i) {
			case SI_PAGE: s = "SI_PAGE"; break;
			case SI_GET_PAGE_ELEM: s = "SI_GET_PAGE_ELEM"; break;
			case SI_GET_REV_CONTENT: s = "SI_GET_REV_CONTENT"; break;
			case SI_WAIT_CLOSE: s = "SI_WAIT_CLOSE"; break;
			case SI_READ_INT: s = "SI_READ_INT"; break;
			case SI_READ_STR: s = "SI_READ_STR"; break;
			case SI_CONSTRUCT_TITLE: s = "SI_CONSTRUCT_TITLE"; break;
			case SI_CONSTRUCT_PAGE: s = "SI_CONSTRUCT_PAGE"; break;
			case SI_FINALIZE_ONE: s = "SI_FINALIZE_ONE"; break;
			default: s="Unknown Insn"; break;
		}
		printf("\t%s\n", s);
	}
	printf("------------ DATA STACK -----------\n");
	for(list<parsingStackElement>::iterator i=st->stack.begin();i != st->stack.end();i++) {
		switch(i->type) {
			case SE_STRING:
				printf("\tSTRING {'%s'}\n", i->str.c_str());
				break;
			case SE_TITLE:
				printf("\tTITLE {'%s'}\n", i->str.c_str());
				break;
			case SE_REDIRECT:
				printf("\tREDIRECT {'%s'}\n", i->str.c_str());
				break;
			case SE_INTEGER:
				printf("\tINTEGER {%d}\n", i->integer);
				break;
			case SE_PAGE:
				printf("\tPAGE {\n");
				{
					Page* p = (Page*)i->object;
					printf("\t\ttitle = '%s',\n", p->title.c_str());
					printf("\t\twikiID = %d,\n", p->wikiID);
					if(p->isRedirect) {
						printf("\t\tredirect = '%s'\n", p->target.c_str());
					} else {
						printf("\t\tlinks = [omitted]\n");
					};
				}
				printf("\t}\n");
				break;
		}
	}
}

const xmlChar* getAttribute(const xmlChar** attrs, xmlChar* attrName) {
	for(int i=0;attrs[i] != NULL;i += 2) {
		const xmlChar* key = attrs[i];
		const xmlChar* value = attrs[i+1];
		if(xmlStrEqual(key, attrName)) return value;
	}
	return NULL;
}

void convertLowerInplace(string& s) {
	for(string::iterator i=s.begin();i != s.end();i++)
		*i = tolower(*i);
}

struct linkListBuilder {
	LinkList links;
	void operator()(const boost::match_results<std::string::const_iterator>& elem) {
		Link l;
		l.target = new string(elem.str(1));
		convertLowerInplace(*l.target);
		l.resolved = false;
		links.push_back(l);
	}
};

void parseLinks(string s, LinkList& l) {
	const boost::regex linkRE("\\[\\[([^|\\]]+)(?:\\|[^\\]]+)?\\]\\]", boost::regex::perl);
	boost::sregex_iterator itr(s.begin(), s.end(), linkRE);
	boost::sregex_iterator end;
	linkListBuilder builder;
	builder = for_each(itr, end, builder);
	l.swap(builder.links);
}

void saxStartElem(void* ctx, const xmlChar* fullname, const xmlChar** attrs) {
	static xmlChar* tagPage = BAD_CAST("page");
	static xmlChar* tagTitle = BAD_CAST("title");
	static xmlChar* tagID = BAD_CAST("id");
	static xmlChar* tagRedirect = BAD_CAST("redirect");
	static xmlChar* tagRevision = BAD_CAST("revision");
	static xmlChar* tagText = BAD_CAST("text");
	parsingState* pr = (parsingState*)ctx;
	if(pr->err) return;
	list<StackInstruction>& insns = pr->insns;
	bool term = true;
	bool acted;
	pr->tagContent.clear();
	while(term) {
		StackInstruction lastInsn = insns.back();
		acted = false;
		switch(lastInsn) {
			case SI_PAGE:
				if(xmlStrEqual(fullname, tagPage)) {
					acted = true;
					STACK_DEBUG("Popping SI_PAGE\n");
					insns.pop_back();
					insns.push_back(SI_PAGE);
					insns.push_back(SI_FINALIZE_ONE);
					insns.push_back(SI_CONSTRUCT_PAGE);
					insns.push_back(SI_GET_PAGE_ELEM);
					insns.push_back(SI_GET_PAGE_ELEM);
					insns.push_back(SI_GET_PAGE_ELEM);
					term = false;
				} else term=false;
				break;
			case SI_GET_PAGE_ELEM:
				if(xmlStrEqual(fullname, tagTitle)) {
					acted = true;
					STACK_DEBUG("Popping SI_GET_PAGE_ELEM due to 'title' tag\n");
					insns.pop_back();
					insns.push_back(SI_CONSTRUCT_TITLE);
					insns.push_back(SI_READ_STR);
					term = false;
				} else if(xmlStrEqual(fullname, tagID)) {
					acted = true;
					STACK_DEBUG("Popping SI_GET_PAGE_ELEM due to 'id' tag\n");
					insns.pop_back();
					insns.push_back(SI_READ_INT);
					term = false;
				} else if(xmlStrEqual(fullname, tagRedirect)) {
					acted = true;
					STACK_DEBUG("Popping SI_GET_PAGE_ELEM due to 'redirect' tag\n");
					insns.pop_back();

					// Read the title attribute
					const xmlChar* titleAttr = getAttribute(attrs, tagTitle);
					parsingStackElement elem;
					elem.type = SE_REDIRECT;
					if(titleAttr == NULL) {
						elem.str = "";
					} else {
						elem.str = string((const char*)titleAttr);
					}
					pr->stack.push_back(elem);
					term = false;
				} else if(xmlStrEqual(fullname, tagRevision)) {
					acted = true;
					STACK_DEBUG("Popping SI_GET_PAGE_ELEM due to 'revision' tag\n");
					insns.pop_back();
					insns.push_back(SI_READ_STR);
					insns.push_back(SI_GET_REV_CONTENT);
					term = false;
				} else term=false;
				break;
			case SI_GET_REV_CONTENT:
				if(xmlStrEqual(fullname, tagText)) {
					acted = true;
					STACK_DEBUG("Popping SI_GET_REV_CONTENT due to 'text' tag\n");
					insns.pop_back();
				} else term=false;
				break;
			case SI_CONSTRUCT_PAGE: {
					acted = true;
					insns.pop_back();
					STACK_DEBUG("Popping and executing SI_CONSTRUCT_PAGE\n");
					// Pull 3 items off the stack
					string title;
					uint64_t wikiID;
					parsingStackElement other;
					other.type = SE_STRING;
					parsingStackElement tmp;
					tmp = pr->stack.back();
					pr->stack.pop_back();
					if(tmp.type == SE_INTEGER) wikiID = tmp.integer;
						else if(tmp.type == SE_TITLE) title = tmp.str;
						else if(tmp.type == SE_STRING) other = tmp;
						else if(tmp.type == SE_REDIRECT) other = tmp;
					tmp = pr->stack.back();
					pr->stack.pop_back();
					if(tmp.type == SE_INTEGER) wikiID = tmp.integer;
						else if(tmp.type == SE_TITLE) title = tmp.str;
						else if(tmp.type == SE_STRING) other = tmp;
						else if(tmp.type == SE_REDIRECT) other = tmp;
					tmp = pr->stack.back();
					pr->stack.pop_back();
					if(tmp.type == SE_INTEGER) wikiID = tmp.integer;
						else if(tmp.type == SE_TITLE) title = tmp.str;
						else if(tmp.type == SE_STRING) other = tmp;
						else if(tmp.type == SE_REDIRECT) other = tmp;

					// Build a page
					Page *p = new Page();
					p->isRedirect = other.type == SE_REDIRECT;
					p->title = title;
					convertLowerInplace(p->title);
					p->wikiID = wikiID;
					STACK_DEBUG("\tConstructed page '%s' id=%d\n", title.c_str(), wikiID);
					if(p->isRedirect) {
						p->target = other.str;
						convertLowerInplace(p->target);
					} else parseLinks(other.str, p->links);
					p->fullyResolved = !p->isRedirect;
					
					// Push the page
					tmp.type = SE_PAGE;
					tmp.object = p;
					pr->stack.push_back(tmp);
				}
				break;
			case SI_WAIT_CLOSE:
				term = false;
				break;
			case SI_READ_STR:
				term = false;
				break;
			case SI_READ_INT:
				term = false;
				break;
			case SI_FINALIZE_ONE:
				acted = true;
				insns.pop_back();
				if(pr->stack.back().type != SE_PAGE) {
					fprintf(stderr, "Warning: Attempt to emit nonpage object\n");
					pr->stack.pop_back();
					break;
				}
				Page* p = (Page*)(pr->stack.back().object);

				// Assign an ID
				uint32_t pID = pr->idsLinks.size();

				// Add to data structures
				pr->idsLinks.push_back(p->links);
				pr->namesIDs[p->title] = pID;

				// Append to the content map
				if(pr->nParsed++ % 1024 == 0) {
					printf("\rParsing XML dump... [%d records done]", pr->nParsed);
					fflush(stdout);
				}
				pr->stack.pop_back();
				break;
			case SI_CONSTRUCT_TITLE:
				acted = true;
				STACK_DEBUG("Executing SI_CONSTRUCT_TITLE\n");
				insns.pop_back();
				pr->stack.back().type = SE_TITLE;
				break;
			default:
				fprintf(stderr, "\nError: Unknown XML stack instruction\n");
				term = false;
				break;
		}
		//if(acted) printStack(pr);
	}
}

void saxEndElem(void* ctx, const xmlChar* name) {
	parsingState* pr = (parsingState*)ctx;
	if(pr->err) return;
	list<StackInstruction>& insns = pr->insns;
	StackInstruction lastInsn = insns.back();
	parsingStackElement elem;
	switch(lastInsn) {
		case SI_READ_STR:
			insns.pop_back();
			STACK_DEBUG("Executing SI_READ_STR\n");
			// Strip whitespace
			pr->tagContent.erase(0,pr->tagContent.find_first_not_of(" \t\r\n"));
			elem.type = SE_STRING;
			elem.str = pr->tagContent;
			//STACK_DEBUG("\tParsed string: '%s'\n", elem.str.c_str());
			pr->stack.push_back(elem);
			break;
		case SI_READ_INT:
			insns.pop_back();
			STACK_DEBUG("Executing SI_READ_INT\n");
			elem.type = SE_INTEGER;
			elem.integer = atol(pr->tagContent.c_str());
			STACK_DEBUG("\tParsed int: %d\n", elem.integer);
			pr->stack.push_back(elem);
			break;
		case SI_WAIT_CLOSE:
			STACK_DEBUG("Executed SI_WAIT_CLOSE\n");
			insns.pop_back();
			break;
		default:
			break;
	}
}

void saxChars(void* ctx, const xmlChar* ch, int len) {
	parsingState* pr = (parsingState*)ctx;
	pr->tagContent.append((const char*)ch, len);
}

void saxError(void* ctx, const char* msg, ...) {
	va_list args;
	va_start(args, msg);
	vprintf(msg, args);
	va_end(args);
}

void linkResolver(parsingState* st, bool& term) {
	bool didResolve = false;
	bool didRegister = false;
	while((!term) || didResolve) {
		// Iterate through finalPages and try to resolve links
		uint32_t ident = 0;
		didResolve = false;
		didRegister = false;
		printf("Got work\n");
		for(synclist<Page*>::iterator i=st->finalPages.begin();i != st->finalPages.end();ident++,++i) {
			Page* p = *i;
			// Add the page to the name mapping
			if(st->nameMapping.find(p->title) == st->nameMapping.end()) {
				st->nameMapping[p->title] = ident;
				didRegister = true;
			}

			// Try to resolve the page's links
			if(p->fullyResolved) continue;
			bool hasUnresolved = false;
			didResolve = true;
			for(LinkList::iterator j=p->links.begin();j != p->links.end();j++) {
				Link& l = *j;
				if(l.resolved) continue;
				// Try to resolve the name
				map<string,uint32_t>::iterator nitr = st->nameMapping.find(*l.target);
				if(nitr != st->nameMapping.end()) {
					// We found the page, so adjust the link
					l.ident = nitr->second;
					l.resolved = true;
					printf("Resolved\n");
					delete l.target;
				} else {
					hasUnresolved = true;
				}
			}
			if(!hasUnresolved) p->fullyResolved = true;
		}
		if((!didRegister) && (!didResolve) && term) break;
		this_thread::sleep_for(chrono::seconds(2));
	}
	printf("Resolver terminating\n");
}

int main(int argc, char **argv) {
	// Prepare state
	parsingState parseRes;
	parsingResult fres;

	// Initialize the SAX handler
	xmlSAXHandler handler;
	memset(&handler, 0, sizeof(handler));
	handler.initialized = XML_SAX2_MAGIC;
	handler.startElement = &saxStartElem;
	handler.endElement = &saxEndElem;
	handler.characters = &saxChars;
	handler.warning = &saxError;
	handler.error = &saxError;
	
	// And parse
	bool parsingDone = false;
	thread resolver(linkResolver, &parseRes, ref(parsingDone));
	int res = xmlSAXUserParseFile(&handler, &parseRes, fname);
	if(res != 0) {
		fres.rc = 1;
		fres.errMsg = "Failed to parse XML";
		return fres;
	}
	parsingDone = true;
	printf("\nXML parsing complete. Waiting for link resolution...");
	resolver.join();
	printf("Done\nParsing complete\n");

	// TEMPORARY FOR MEMORY OPTIMIZATION
	printf("Length: %d\n", parseRes.finalPages.size());
	for(synclist<Page*>::iterator i=parseRes.finalPages.begin();i != parseRes.finalPages.end();++i) {
		//printf("Page:\n\tTitle: %s\n\tLinks:\n", (*i)->title.c_str());
		for(LinkList::iterator j=(*i)->links.begin();j != (*i)->links.end();j++) {
			if(j->resolved) {
				//printf("\t\tResolved: %d\n", j->ident);
			} else {
				//printf("\t\tUnresolved: '%s'\n", j->target->c_str());
			}
		}
		delete *i;
	}

	// Construct the ID map

	// Copy output
	fres.rc = 0;
}
