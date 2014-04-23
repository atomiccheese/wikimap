#pragma once
#include <functional>
#include <stdexcept>
#include <stdio.h>

template<typename I, typename T>
struct RBNode {
	RBNode* left;
	RBNode* right;
	RBNode* parent;
	bool color;
	I value;
	T payload;

	~RBNode() {
		if(left != NULL) delete left;
		if(right != NULL) delete right;
		left = NULL;
		right = NULL;
	}
};

template<typename I, typename T, class Cmp=std::less<I> >
class RBTree {
	typedef RBNode<I,T>* payload;
	payload root;
	Cmp cmp;
public:
	RBTree() {
		root = NULL;
	}

	~RBTree() {
		delete root;
	}

	payload getRoot() {
		return root;
	}

	void insert(I value, T payload) {
		// Allocate a new RBNode
		RBNode<I,T>* newNode = new RBNode<I,T>();
		newNode->left = newNode->right = newNode->parent = NULL;
		newNode->color = true;
		newNode->value = value;
		newNode->payload = payload;

		// Make sure the root is okay
		if(root == NULL) {
			root = newNode;
			root->color = false;
			return;
		}

		// Insert
		RBNode<I,T>* n = root;
		while(true) {
			if((!cmp(value, n->value)) && (!cmp(n->value, value))) {
				throw std::runtime_error("Already present");
			} else if(cmp(value, n->value)) {
				if(n->left == NULL) {
					n->left = newNode;
					newNode->parent = n;
					break;
				} else {
					n = n->left;
				}
			} else {
				if(n->right == NULL) {
					n->right = newNode;
					newNode->parent = n;
					break;
				} else {
					n = n->right;
				}
			}
		}

		// Adjust for constraints
		insert_case1(newNode);
	}

	T search(I value) {
		// Basic binary-tree search
		payload n = root;
		while(n != NULL) {
			if(cmp(value, n->value)) {
				n = n->left;
			} else if(cmp(n->value, value)) {
				n = n->right;
			} else {
				return n->payload;
			}
		}
		throw std::runtime_error("No value for key " + value);
	}

	bool contains(I value) {
		// Basic binary-tree search
		payload n = root;
		while(n != NULL) {
			if(cmp(value, n->value)) {
				n = n->left;
			} else if(cmp(n->value, value)) {
				n = n->right;
			} else {
				return true;
			}
		}
		return false;
	}

private:
	payload grandparent(payload node) {
		if((node != NULL) && (node->parent != NULL))
			return node->parent->parent;
	}

	payload uncle(payload node) {
		payload g = grandparent(node);
		if(g == NULL) return NULL;
		if(g->left == node->parent) {
			return g->right;
		} else {
			return g->left;
		}
	}

	void insert_case1(payload node) {
		if(node == root) {
			node->color = false;
		} else {
			insert_case2(node);
		}
	}

	void rot_left(payload node) {
		payload p = node->right;

		// Replace node with p
		if(root == node) {
			root = p;
			p->parent = NULL;
		} else {
			if(node == node->parent->left) {
				node->parent->left = p;
			} else {
				node->parent->right = p;
			}
			p->parent = node->parent;
		}

		// Make node a child of p and adjust their children
		node->parent = p;
		node->right = p->left;
		p->left = node;

		if(node->right != NULL) {
			node->right->parent = node;
		}
	}

	void rot_right(payload node) {
		payload p = node->left;

		if(root == node) {
			root = p;
			p->parent = NULL;
		} else {
			if(node == node->parent->left) {
				node->parent->left = p;
			} else {
				node->parent->right = p;
			}
			p->parent = node->parent;
		}

		// Make node a child of p and adjust their children
		node->parent = p;
		node->left = p->right;
		p->right = node;
		
		if(node->left != NULL) node->left->parent = node;
	}

	void insert_case2(payload node) {
		if(!node->parent->color) {
			return;
		} else {
			insert_case3(node);
		}
	}

	void insert_case3(payload node) {
		payload unc = uncle(node);
		if((unc != NULL) && (unc->color)) {
			node->parent->color = false;
			unc->color = false;
			unc->parent->color = true;
			insert_case1(grandparent(node));
		} else {
			insert_case4(node);
		}
	}

	void insert_case4(payload node) {
		payload g = grandparent(node);

		if(g == NULL) return;
		if((node->parent->right == node) && (g->left == node->parent)) {
			rot_left(node->parent);
			insert_case5(node->left);
		} else if((node->parent->left == node) && (g->right == node->parent)) {
			rot_right(node->parent);
			insert_case5(node->right);
		} else {
			insert_case5(node);
		}
	}

	void insert_case5(payload node) {
		payload g = grandparent(node);
		node->parent->color = true;
		g->color = true;
		if(node == node->parent->left) {
			rot_right(g);
		} else {
			rot_left(g);
		}
	}
};

