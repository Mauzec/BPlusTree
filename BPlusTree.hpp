#ifndef BPlusTree_hpp
#define BPlusTree_hpp

#include <iostream>
#include <string>
#include <assert.h>
#include <vector>

class Entry {
public:
    std::string birth;
    std::string homeBlock;
    std::string homeRoom;
    std::string fac;
    const char *name;
    
public:
    Entry (): birth(""), homeBlock(""), homeRoom(""), fac(""), name("None") {}
    Entry (std::string b, std::string hb, std::string hr, std::string f, const char *n = "None") {
        birth = b;
        homeBlock = hb;
        homeRoom = hr;
        fac = f;
        name = n;
    }
    
    void out (bool printName = false) {
        std::cout << birth << '\n';
        std::cout << homeBlock << '\n';
        std::cout << homeRoom << '\n';
        std::cout << fac << '\n';
        if (printName) std::cout << name << '\n';
    }
};

namespace BPT {

#define TREE_ORDER 20
#define OFFSET_META 0
#define OFFSET_BLOCK OFFSET_META + sizeof(metaT)
#define SIZEWITHNOCHILD sizeof(leafT) - TREE_ORDER * sizeof(recordT)

typedef Entry valueT;

// key struct
struct keyT {
    char K[20];
    
    keyT (const char *str = "") {
        bzero(K, sizeof(K));
        strcpy(K, str);
    }
    
    operator bool () const {
        return strcmp(K, "");
    }
};

// key comparator
inline int keycmp (const keyT &a, const keyT &b) {
    int delta = int((strlen(a.K)) - (strlen(b.K)));
    
    return delta == 0 ? strcmp(a.K, b.K) : delta;
}

// some operators of comparing
#define keycmpOperator(type) \
    bool operator < (const keyT &l, const type &r) {\
        return keycmp(l, r.key) < 0;\
    }\
    bool operator < (const type &l, const keyT &r) {\
        return keycmp(l.key, r) < 0;\
    }\
    bool operator == (const keyT &l, const type &r) {\
        return keycmp(l, r.key) == 0;\
    }\
    bool operator == (const type &l, const keyT &r) {\
        return keycmp(l.key, r) == 0;\
    }

typedef off_t offT;
typedef size_t sizeT;


// nodes' index segment
struct indexT {
    keyT key;
    offT child;
};

// node block
struct nodeT {
    typedef indexT *childT;
 
    // parent node
    offT parent;
    
    offT next, prev;
    
    sizeT countChilds;
    indexT child[TREE_ORDER];
};

// final record (leaf)
struct recordT {
    keyT key;
    valueT value;
};

// leaf block
struct leafT {
    typedef recordT *childT;
    
    offT parent;
    offT next, prev;
    sizeT countChilds;
    recordT child[TREE_ORDER];
};

// info of b+ tree
typedef struct {
    sizeT order; // B+ tree order
    sizeT valueSize; // size of value
    sizeT keySize; // size of key
    sizeT countNode; // count of internal nodes in the tree
    sizeT countLeaf; // count of leafs in the tree
    sizeT height; // height of tree (MARK: leafs' not included in this number)
    offT slot; // place of storing new block
    offT rootOffset; // place of root of internal nodes
    offT leafOffset; // place of the first leaf
} metaT;

// b+ tree!
class BPlusTree {
private:
    char filePath [512];
    metaT meta;
    
    // !!! Only for experemen. purposes
    void initEmpty (); // init empty tree
    
    // searching index
    offT searchIndex (const keyT &key) const;
    
    // searching leaf
    offT searchLeaf (offT index, const keyT &key) const;
    offT searchLeaf (const keyT &key) const {return searchLeaf(searchIndex(key), key);}
    
    // removing node
    void removeFromIndex (offT offset, nodeT &node, const keyT &key);
    
    // merge leafs right to left;
    void mergeLeafs (leafT *left, leafT *right);
    void mergeKeys (indexT *place, nodeT &left, nodeT &right, bool changeWhereKey = false);
    
    // insert key to the node
    void insertKeyToIndex (offT offset, const keyT &key, offT old, offT after);
    void insertKeyToIndexNoSplit (nodeT &node, const keyT &key, offT value);
    // insert to leaf (without split)
    void insertRecordNoSplit (leafT *leaf, const keyT &key, const valueT &value);
    
    // borrow a key from other node
    bool borrowKey (bool fromRight, nodeT &from, offT offset);
    // borrow a record from other leaf
    bool borrowKey (bool fromRight, leafT &from);
    
    // change parent of children
    void resetIndexChildParent (indexT *begin, indexT *end, offT parent);
    
    // change parent key
    void changeChildParent (offT parent, const keyT &old, const keyT &newKey);
    
    template <class T> void createNode (offT offset, T *node, T *next);
    template <class T> void removeNode (T *prev, T *node);
    
    
    // multi level file (opening/reading/writing/closing)
    mutable FILE *F;
    mutable int fileLevel;
    
    void openFile (const char *mode = "rb+") const;
    void closeFile () const;
    
    // allocation from disk
    offT alloc (sizeT size);
    offT alloc (leafT *leaf);
    offT alloc (nodeT *node);
    
    void unalloc (leafT *leaf, offT offset);
    void unalloc (nodeT *node, offT offset);
    
    // read block from disk
    int map (void *block, offT offset, sizeT size) const;
    template <class T> int map (T *block, offT offset) const;
    
    // write block into disk
    int unmap (void *block, offT offset, sizeT size) const;
    
    template <class T> int unmap (T *block, offT offset) const;
    
public:
    BPlusTree (const char *filePath, bool force = false);
    
    // basic methods of tree
    int search (const keyT &key, valueT *value) const;
    int searchSegment (keyT *a, const keyT &b, valueT *values, sizeT max, bool *next = nullptr) const;
    
    int insert (const keyT &key, valueT value);
    int remove (const keyT &key);
    int update (const keyT &key, valueT value);
    
    metaT getInfo () const {return meta;}
};


}



#endif /* BPlusTree_hpp */
