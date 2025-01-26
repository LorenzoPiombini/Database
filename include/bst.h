#ifndef _BST_H_
#define _BST_H_

#define LEFT 0
#define RIGHT 1
#define LR_ROT 10
#define RL_ROT 11

/* macro for declaring the binary search tree*/
#define BST_init BST binary_st = {NULL, insert_bst}

/* macro to insert element in the bst */
#define BST_insert(TYPE, VALUE, NODE, DATA, DATA_TYP) binary_st.insert(TYPE, VALUE, NODE, DATA, DATA_TYP)

/* macro to access the binary tree */
#define BST_tree binary_st

/* macro to pass the tree to functions*/
#define BST_param BST BST_tree

/* macro to pass a pointer to a tree to functions*/
#define pBST_param BST *BST_tree

typedef enum
{
    t_i,
    t_s,
    t_l,
    rec_t,
    t_f,
    t_d,
    t_v, /* type void* */
    t_NL
} Types;

typedef struct Node_bst
{
    Types type;
    union
    {
        int i;
        long l;
        char *s;
        void *ptr;
    } value;
    struct Node_bst *left;
    struct Node_bst *right;
    void *data; /*could be Record_f or HashTable or anything else*/
} Node_bst;

typedef struct BST
{
    Node_bst *root;
    unsigned char (*insert)(Types type, void *value, Node_bst **node, void **data, Types data_typ);
} BST;

Node_bst *create_node(Types type, void *value, void **data, Types data_typ);
unsigned char insert_bst(Types type, void *value, Node_bst **current, void **data, Types data_typ);
unsigned char find(Types type, void *value, Node_bst **current, void **data, Types data_typ);
void free_node(Node_bst *node);
void free_BST(BST *binary_st);
int height(Node_bst *node, int branch);

#endif
