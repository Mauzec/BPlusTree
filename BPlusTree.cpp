#include "BPlusTree.hpp"


// IMPLEMENTATION OF B-PLUS-TREE

namespace BPT {

keycmpOperator (indexT);
keycmpOperator (recordT);

/* PART: file methods */
/* opening file */
void  BPlusTree::openFile (const char *mode) const {
    // MARK: \\
    /* so "rb+" (default mode in this case) help us to write data with no truncating */
    /* but you can always change "mode" for your purposes */
    
    if (fileLevel == 0) F = fopen(filePath, mode);
    
    ++fileLevel;
}
/* closing file */
void  BPlusTree::closeFile () const {
    if (fileLevel == 1) fclose(F);
    
    --fileLevel;
}
/* allocation the place in the file */
offT  BPlusTree::alloc (sizeT size) {
    offT slot = meta.slot;
    meta.slot += size; /* shift the slot for the future alocations */
    return slot;
}
/* allocation the place for leaf */
/* it uses alloc(sizeT size) */
offT  BPlusTree::alloc (leafT *leaf) {
    leaf->countChilds = 0;
    ++meta.countLeaf;
    return alloc(sizeof(leafT));
}
/* allocation the place for node */
/* it uses alloc(sizeT size) */
offT  BPlusTree::alloc (nodeT *node) {
    node->countChilds = 1;
    ++meta.countNode;
    return alloc(sizeof(nodeT));
}
/* MARK: to do ? */
void  BPlusTree::unalloc (leafT *leaf, offT offset) {
    --meta.countLeaf;
}
/* MARK: to do ? */
void  BPlusTree::unalloc (nodeT *node, offT offset) {
    --meta.countNode;
}
/* reading the file and writing read one out in the "block" */
int  BPlusTree::map (void *block, offT offset, sizeT size) const {
    openFile();
    fseek(F, offset, SEEK_SET);
    sizeT R = fread(block, size, 1, F);
    closeFile();
    return int(R) - 1;
}
/* map function for lazy >3 */
template <class T>
int  BPlusTree::map (T *block, offT offset) const {
    return map(block, offset, sizeof(T));
}
/* writing the "block" out in the file  */
int  BPlusTree::unmap (void *block, offT offset, sizeT size) const {
    openFile();
    fseek(F, offset, SEEK_SET);
    sizeT W = fwrite(block, size, 1, F);
    closeFile();
    
    return int(W) - 1;
}

/* umap for lazy >3 */
template <class T>
int  BPlusTree:: unmap (T *block, offT offset) const {
    return unmap(block, offset, sizeof(T));
}

/* ---------------------- */

/* PART: HELPERS FUNCTIONS */
/* SUBPART: Iterators */
/* return children[0] */
template <class T>
inline typename T::childT begin (T &node) {
    return node.child;
}
/* return children[last] */
template <class T>
inline typename T::childT end (T &node) {
    return node.child + node.countChilds;
}

/* SUBPART: Looking for ... */
/* key in the node */
inline indexT *find (nodeT &node, const keyT &key) {
    if (key) return std::upper_bound(begin(node), end(node) - 1, key); // return child key < node' child element
    
    // return second last child (if we search the empty key)
    if (node.countChilds > 1) return node.child + node.countChilds - 2;
    
    return begin(node);
}

/* key in the leaf */
inline recordT *find (leafT &leaf, const keyT &key) {
    return std::lower_bound(begin(leaf), end(leaf), key);
}

/* PART: BPLUS-TREE FUNCTIONS */
/* Initialization from empty file*/
void  BPlusTree::initEmpty () {
    bzero(&meta, sizeof(metaT));
    meta.order = TREE_ORDER;
    meta.valueSize = sizeof(valueT);
    meta.keySize = sizeof(keyT);
    meta.height = 1;
    meta.slot = OFFSET_BLOCK;
    
    /* initialization of root node */
    nodeT root; root.parent = 0;
    root.next = root.prev = root.parent;
    meta.rootOffset = alloc(&root);
    
    // initialization of empty leaf
    leafT leaf; leaf.parent = meta.rootOffset;
    leaf.next = leaf.prev = 0;
    meta.leafOffset = root.child[0].child = alloc(&leaf);
    
    // saving in the file
    unmap(&meta, OFFSET_META);
    unmap(&root, meta.rootOffset);
    unmap(&leaf, root.child[0].child);
}


/* Constructor */
BPlusTree::BPlusTree (const char *newPath, bool forceEmpty) : F(nullptr), fileLevel(0) {
    bzero(filePath, sizeof(filePath));
    strcpy(filePath, newPath);
    
    /* reading tree */
    if (!forceEmpty) { if (map(&meta, OFFSET_META) != 0) forceEmpty = true; }
    else {
        // to truncate file
        openFile("w+");
        initEmpty();
        closeFile();
    }
}

/* Searching index(offset) of key */
 offT BPlusTree::searchIndex (const keyT &key) const {
    offT off = meta.rootOffset;
    sizeT height = meta.height;
    
    while (height > 1) {
        nodeT node; map(&node, off);
        
        indexT *ind = std::upper_bound(begin(node), end(node) - 1, key);
        
        off = ind->child;
        --height;
    }
    
    return off;
}
/* Searching index(offset) of leaf */
offT  BPlusTree::searchLeaf (offT index, const keyT &key) const {
    nodeT node; map(&node, index);
    
    indexT *ind = std::upper_bound(begin(node), end(node) - 1, key);
    return ind->child;
}

/* Searching leaf by key */
 int  BPlusTree::search (const keyT &key, valueT *value) const {
    leafT leaf; map(&leaf, searchLeaf(key));
    
    recordT *record = find(leaf, key);
    if (record != leaf.child + leaf.countChilds) {
        *value = record->value;
        return keycmp(record->key, key);
    } else return -1;
}
/* Searching values by key[left:right] */
 int  BPlusTree::searchSegment (keyT *left, const keyT &right, valueT *values, sizeT max, bool *next) const {
    if (left == nullptr || keycmp(*left, right) > 0)
        return -1;
    
    offT offLeft = searchLeaf(*left);
    offT offRight = searchLeaf(right);
    
    offT off = offLeft;
    
    sizeT k = 0;
    recordT *b = nullptr, *e = nullptr;
    
    leafT leaf;
    
    while (off != offRight && off != 0 && k < max) {
        map(&leaf, off);
        
        // starting
        if (offLeft == off)
            b = find(leaf, *left);
        else
            b = begin(leaf);
        
        /* copying */
        e = leaf.child + leaf.countChilds;
        while (b != e && k < max) {
            values[k] = b->value;
            ++b; ++k;
        }
        
        off = leaf.next;
    }
    
    /* last leaf */
    if (k < max) {
        map(&leaf, offRight);
        
        b = find(leaf, *left);
        e = std::upper_bound(begin(leaf), end(leaf), right);
        while (b != e && k < max) {
            values[k] = b->value;
            ++b; ++k;
        }
    }
    
    /* for the future */
    if (next != nullptr) {
        if (k == max && b != e) {
            *next = true;
            *left = b->key;
        } else *next = false;
    }
    
    return int(k);
}
/* reset parents of nodes[begin:end] */
void  BPlusTree::resetIndexChildParent (indexT *begin, indexT *end, offT parent) {
    nodeT node;
    while (begin != end) {
        map(&node, begin->child);
        node.parent = parent;
        unmap(&node, begin->child, SIZEWITHNOCHILD);
        ++begin;
    }
    
}
/* change leaf[key] to value */
int  BPlusTree::update(const keyT& key, valueT value)
{
    offT offset = searchLeaf(key);
    leafT leaf;
    map(&leaf, offset);
    
    recordT *record = find(leaf, key);
    if (record != leaf.child + leaf.countChilds)
        if (keycmp(key, record->key) == 0) {
            record->value = value;
            unmap(&leaf, offset);
            
            return 0;
        } else {
            return 1;
        }
        else
            return -1;
}


void  BPlusTree::changeChildParent (offT parent, const keyT &old, const keyT &newKey) {
    nodeT node;
    map(&node, parent);
    
    indexT *indNode = find(node, old);
    assert(indNode != node.child + node.countChilds);
    
    indNode->key = newKey;
    unmap(&node, parent);
    
    if (indNode == node.child + node.countChilds - 1)
        changeChildParent(node.parent, old, newKey);
}

bool  BPlusTree::borrowKey(bool fromRight, leafT &borrower)
{
    offT lender_off = fromRight ? borrower.next : borrower.prev;
    leafT lender;
    map(&lender, lender_off);
    
    assert(lender.countChilds >= meta.order / 2);
    if (lender.countChilds != meta.order / 2) {
        typename leafT::childT whereToLend = nullptr, whereToPut;
        
        /* decide offset and update parent's key */
        if (fromRight) {
            whereToLend = begin(lender);
            whereToPut = end(borrower);
            changeChildParent(borrower.parent, begin(borrower)->key,
                              lender.child[1].key);
        } else {
            whereToLend = end(lender) - 1;
            whereToPut = begin(borrower);
            changeChildParent(lender.parent, begin(lender)->key,
                              whereToLend->key);
        }
        
        /* storing */
        std::copy_backward(whereToPut, end(borrower), end(borrower) + 1);
        *whereToPut = *whereToLend;
        borrower.countChilds++;
        
        /* erasing */
        std::copy(whereToLend + 1, end(lender), whereToLend);
        lender.countChilds--;
        unmap(&lender, lender_off);
        return true;
    }
    
    return false;
}

bool  BPlusTree::borrowKey (bool fromRight, nodeT &from, offT off) {
    typedef typename nodeT::childT childT;
    
    offT offLender = fromRight ? (from.next) : (from.prev);
    nodeT lender;
    map(&lender, offLender);
    
    assert(lender.countChilds >= meta.order / 2);
    
    if (lender.countChilds != meta.order / 2) {
        childT whereLend, wherePut;
        nodeT parent;
        
        /* swapping keys */
        if (fromRight) {
            whereLend = begin(lender);
            wherePut = end(from);
            
            map(&parent, from.parent);
            childT where = std::lower_bound(begin(parent), end(parent) - 1, (end(from) - 1)->key);
            
            where->key = whereLend->key;
            unmap(&parent, from.parent);
        } else {
            whereLend = end(lender) - 1;
            wherePut = begin(from);
            
            map(&parent, lender.parent);
            childT where = find(parent, begin(lender)->key);
            
            where->key = (whereLend - 1)->key;
            unmap(&parent, lender.parent);
        }
        
        /* storing */
        std::copy_backward(wherePut, end(from), end(from) + 1);
        *wherePut = *whereLend;
        ++from.countChilds;
        
        /* erasing */
        resetIndexChildParent(whereLend, whereLend + 1, off);
        std::copy(whereLend + 1, end(lender), whereLend);
        
        --lender.countChilds;
        
        unmap(&lender, offLender);
        return true;
    }
    
    return false;
}

void  BPlusTree::mergeKeys(indexT *where,
                                nodeT &node, nodeT &next, bool changeKey) {
    if (changeKey) {
        where->key = (end(next) - 1)->key;
    }
    std::copy(begin(next), end(next), end(node));
    node.countChilds += next.countChilds;
    removeNode(&node, &next);
}

void  BPlusTree::mergeLeafs(leafT *left, leafT *right)
{
    std::copy(begin(*right), end(*right), end(*left));
    left->countChilds += right->countChilds;
}

void  BPlusTree::removeFromIndex (offT off, nodeT &node, const keyT &key) {
    sizeT minCount;
    if (meta.rootOffset == off) minCount = 1;
    else minCount = meta.order / 2;
    
    assert(node.countChilds >= minCount && node.countChilds <= meta.order);
    
    /* removing key */
    keyT k = begin(node)->key;
    indexT *delet = find(node, key);
    if (end(node) != delet) {
        (delet + 1)->child = delet->child;
        std::copy(delet + 1, end(node), delet);
    }
    --node.countChilds;
    
    /* removing if only key */
    if (node.countChilds == 1 && meta.rootOffset == off && meta.countNode != 1) {
        unalloc(&node, meta.rootOffset);
        meta.height -= 1;
        meta.rootOffset = node.child[0].child;
        unmap(&meta, OFFSET_META);
        return;
    }
    
    /* merging (borrowing) */
    if (node.countChilds < minCount) {
        nodeT parent;
        map(&parent, node.parent);
        
        /* borrow from left */
        bool done = false;
        if (off != begin(parent)->child)
            done = borrowKey(false, node, off);
        
        /* borrow from right */
        if (!done && off != (end(parent) - 1)->child)
            done = borrowKey(true, node, off);
        
        /* merging */
        if (!done) {
            assert(node.next != 0 || node.prev != 0);
            
            if (off == (end(parent) - 1)->child) {
                /* leaf is last --> merge prev-leaf */
                assert(node.prev != 0);
                nodeT prev;
                map(&prev, node.prev);
                
                /* merging */
                indexT *where = find(parent, begin(prev)->key);
                resetIndexChildParent(begin(node), end(node), node.prev);
                mergeKeys(where, prev, node, true);
                unmap(&prev, node.prev);
            } else {
                /* leaf isnt last -->merge leaf-next */
                assert(node.next != 0);
                nodeT next;
                map(&next, node.next);
                
                /* merging */
                indexT *where = find(parent, k);
                resetIndexChildParent(begin(next), end(next), off);
                mergeKeys(where, node, next);
                unmap(&node, off);
            }
            
            /* deleting parent key */
            removeFromIndex(node.parent, parent, k);
        }
        else unmap(&node, off);
    }
    else unmap(&node, off);
}

int  BPlusTree::remove (const keyT &key) {
    nodeT parent;
    leafT leaf;
    
    /* search parent */
    offT parentOff = searchIndex(key);
    map(&parent, parentOff);
    
    /* search node to delete */
    indexT *where = find(parent, key);
    offT off = where->child;
    map(&leaf, off);
    
    /* checking */
    if (!std::binary_search(begin(leaf), end(leaf), key)) return -1;
    
    sizeT minCount;
    if (meta.countLeaf == 1) minCount = 0;
    else minCount = meta.order / 2;
    
    assert(leaf.countChilds >= minCount && leaf.countChilds <= meta.order);
    
    /* removing the key */
    recordT *delet = find(leaf, key);
    std::copy(delet + 1, end(leaf), delet);
    --leaf.countChilds;
    
    /* merging (borrowing) */
    if (leaf.countChilds < minCount) {
        bool done = false;
        
        /* borrow from left */
        if (leaf.prev != 0)
            done = borrowKey(false, leaf);
        
        /* borrow from right */
        if (!done && leaf.next != 0)
            done = borrowKey(true, leaf);
        
        /* merge */
        if (!done) {
            assert(leaf.next != 0 || leaf.prev != 0);
            
            keyT k;
            if (where == end(parent) - 1) {
                /* leaf is last --> merge prev-leaf */
                assert(leaf.prev != 0);
                leafT prev;
                map(&prev, leaf.prev);
                k = begin(prev)->key;
                
                mergeLeafs(&prev, &leaf);
                removeNode(&prev, &leaf);
                unmap(&prev, leaf.prev);
            } else {
                /* leaf isn't last --> merge leaf-next */
                assert(leaf.next != 0);
                leafT next;
                map(&next, leaf.next);
                k = begin(leaf)->key;
                
                mergeLeafs(&leaf, &next);
                removeNode(&leaf, &next);
                unmap(&leaf, off);
            }
            
            /* delete parent key */
            removeFromIndex(parentOff, parent, k);
        }
        else unmap(&leaf, off);
    }
    else unmap(&leaf, off);
    
    return 0;
}

template <class T>
void  BPlusTree::createNode (offT off, T *node, T *next) {
    /* new brother */
    next->parent = node->parent;
    next->next = node->next;
    next->prev = off;
    node->next = alloc(next);
    
    /* changing next node prev */
    if (next->next != 0) {
        T oldNext;
        map(&oldNext, next->next, SIZEWITHNOCHILD);
        oldNext.prev = node->next;
        unmap(&oldNext, next->next, SIZEWITHNOCHILD);
    }
    
    unmap(&meta, OFFSET_META);
}

template <class T>
void  BPlusTree::removeNode (T *prev, T *node) {
    unalloc(node, prev->next);
    prev->next = node->next;
    
    if (node->next != 0) {
        T next;
        map(&next, node->next, SIZEWITHNOCHILD);
        next.prev = node->prev;
        unmap(&next, node->next, SIZEWITHNOCHILD);
    }
    unmap(&meta, OFFSET_META);
}

void  BPlusTree::insertKeyToIndex (offT off, const keyT &key, offT old, offT after) {
    if (off == 0) {
        /* creating new root */
        nodeT root;
        root.next = root.prev = root.parent = 0;
        meta.rootOffset = alloc(&root);
        ++meta.height;
        
        /* inserting old and after */
        root.countChilds = 2;
        root.child[0].key = key;
        root.child[0].child = old;
        root.child[1].child = after;
        
        unmap(&meta, OFFSET_META);
        unmap(&root, meta.rootOffset);
        
        resetIndexChildParent(begin(root), end(root), meta.rootOffset);
        return;
    }
    
    nodeT node;
    map(&node, off);
    assert(node.countChilds <= meta.order);
    
    if (node.countChilds == meta.order) {
        /* full --> split */
        
        nodeT newNode;
        createNode(off, &node, &newNode);
        
        sizeT mid = (node.countChilds - 1) / 2;
        bool toRight = keycmp(key, node.child[mid].key) > 0;
        if (toRight) ++mid;
        
        if (toRight && keycmp(key, node.child[mid].key) < 0) --mid;
        
        keyT midKey = node.child[mid].key;
        
        /* spliting */
        std::copy(begin(node) + mid + 1, end(node), begin(newNode));
        newNode.countChilds = node.countChilds - mid - 1;
        node.countChilds = mid + 1;
        
        /* inserting new key */
        if (toRight) insertKeyToIndexNoSplit(newNode, key, after);
        else insertKeyToIndexNoSplit(node, key, after);
        
        unmap(&node, off);
        unmap(&newNode, node.next);
        
        resetIndexChildParent(begin(newNode), end(newNode), node.next);
        
        insertKeyToIndex(node.parent, midKey, off, node.next);
    } else {
        insertKeyToIndexNoSplit(node, key, after); unmap(&node, off);
    }
}

void  BPlusTree::insertRecordNoSplit (leafT *leaf, const keyT &key, const valueT &value) {
    recordT *where = std::upper_bound(begin(*leaf), end (*leaf), key);
    std::copy_backward(where, end(*leaf), end(*leaf) + 1);
    
    where->key = key;
    where->value = value;
    ++leaf->countChilds;
}

void  BPlusTree::insertKeyToIndexNoSplit (nodeT &node, const keyT &key, offT value) {
    indexT *where = std::upper_bound(begin(node), end(node) - 1, key);
    
    /* moving index forward */
    std::copy_backward(where, end(node), end(node) + 1);
    
    /* inserting key */
    where->key = key;
    where->child = (where + 1)->child;
    (where + 1)->child = value;
    
    ++node.countChilds;
}

int  BPlusTree::insert (const keyT &key, valueT value) {
    offT parent = searchIndex(key);
    offT off = searchLeaf(parent, key);
    leafT leaf;
    map(&leaf, off);
    
    /* have the same key? */
    if (std::binary_search(begin(leaf), end(leaf), key)) return 1;
    
    if (leaf.countChilds == meta.order) {
        /* spliting (because full leaf) */
        
        leafT new_leaf;
        createNode(off, &leaf, &new_leaf);
        
        // find even split point
        size_t point = leaf.countChilds / 2;
        bool place_right = keycmp(key, leaf.child[point].key) > 0;
        if (place_right)
            ++point;
        
        // split
        std::copy(leaf.child + point, leaf.child + leaf.countChilds,
                  new_leaf.child);
        new_leaf.countChilds = leaf.countChilds - point;
        leaf.countChilds = point;
        
        // which part do we put the key
        if (place_right)
            insertRecordNoSplit(&new_leaf, key, value);
        else
            insertRecordNoSplit(&leaf, key, value);
        
        // save leafs
        unmap(&leaf, off);
        unmap(&new_leaf, leaf.next);
        
        // insert new index key
        insertKeyToIndex(parent, new_leaf.child[0].key,
                         off, leaf.next);
    } else {
        insertRecordNoSplit(&leaf, key, value);
        unmap(&leaf, off);
    }
    
    return 0;
}

}
