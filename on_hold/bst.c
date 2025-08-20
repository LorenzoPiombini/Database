#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "bst.h"
#include "str_op.h"
#include "record.h" /* for struct Record_f */
#include "debug.h"

Node_bst *create_node(Types type, void *value, void **data, Types data_typ)
{

    Node_bst *node = calloc(1, sizeof(Node_bst));
    if (!node)
    {
        __er_calloc(F, L - 3);
        return NULL;
    }

    node->type = type;
    if (data)
    {
        switch (data_typ)
        {
        case t_i:
        {
            void *copy_data = calloc(1, sizeof(int));
            if (!copy_data)
            {
                __er_calloc(F, L - 3);
                free(node);
                return NULL;
            }
            memcpy(copy_data, *data, sizeof(int));
            node->data = copy_data;
            break;
        }
        case t_s:
        {
            char *temp = strdup((char *)*data);
            if (!temp)
            {
                printf("strdup failed, %s:%d.\n", F, L - 3);
                free(node);
                return NULL;
            }
            size_t l = strlen(temp);
            void *copy_data = calloc(++l, sizeof(char));
            if (!copy_data)
            {
                __er_calloc(F, L - 3);
                free(node);
                return NULL;
            }
            memcpy(copy_data, (void *)temp, l);
            free(temp);
            node->data = copy_data;
            break;
        }
        case t_l:
        {
            void *copy_data = calloc(1, sizeof(long));
            if (!copy_data)
            {
                __er_calloc(F, L - 3);
                free(node);
                return NULL;
            }
            memcpy(copy_data, *data, sizeof(long));
            node->data = copy_data;
            break;
        }
        case rec_t:
        {
            void *copy_data = calloc(1, sizeof(struct Record_f *));
            if (!copy_data)
            {
                __er_calloc(F, L - 3);
                free(node);
                return NULL;
            }
            memcpy(copy_data, *data, sizeof(struct Record_f *));
            node->data = copy_data;
            break;
        }
        case t_f:
        {
            void *copy_data = calloc(1, sizeof(float));
            if (!copy_data)
            {
                __er_calloc(F, L - 3);
                free(node);
                return NULL;
            }
            memcpy(copy_data, *data, sizeof(float));
            node->data = copy_data;
            break;
        }
        case t_d:
        {
            void *copy_data = calloc(1, sizeof(double));
            if (!copy_data)
            {
                __er_calloc(F, L - 3);
                free(node);
                return NULL;
            }
            memcpy(copy_data, *data, sizeof(double));
            node->data = copy_data;
            break;
        }
        default:
            node->data = NULL;
            break;
        }
    }
    else
    {
        node->data = NULL;
    }

    switch (type)
    {
    case t_i:
        node->value.i = *((int *)value);
        break;
    case t_s:
        node->value.s = strdup((char *)value);
        if (!node->value.s)
        {
            printf("strdup() failed, %s:%d.\n", F, L - 3);
            free(node);
            return 0;
        }
        strip('_', node->value.s);
        break;
    case t_l:
        node->value.l = *((long *)value);
        break;
    case t_v:
        node->value.ptr = value;
        break;
    default:
        printf("unknown value for node in BST.\n");
        free(node);
        return 0;
    }

    return node;
}

unsigned char insert_bst(Types type, void *value, Node_bst **current, void **data, Types data_typ)
{
    if (*current)
    {
        if ((*current)->type != type)
        {
            printf("node type not valid.\n");
            return 0;
        }
    }
    Node_bst *node = create_node(type, value, data, data_typ);
    if (!node)
    {
        return 0;
    }

    if (!*current) /*here is the actual insertion*/
    {
        *current = node;
        return 1;
    }

    switch (type)
    {
    case t_i:
        if (node->value.i > (*current)->value.i)
        {
            if (!insert_bst(node->type, (void *)&node->value.i,
                            &(*current)->right, &node->data, data_typ))
            {
                free_node(node);
                return 0;
            }

            free_node(node);

            /* check if the tree is balanced*/
            int bf = height(*current, RIGHT);
            if (bf > 1 || bf < -1)
            {
                if (bf == -2)
                {
                    /*balance the tree RR-rotation*/
                    Node_bst *temp = (*current)->right;
                    (*current)->right = NULL;
                    temp->left = (*current);
                    (*current) = temp;
                    bf = height(*current, RIGHT);
                    assert(bf < 1 || bf > -1); /*make sure the tree now is balanced*/
                }
                else if (bf == RL_ROT)
                {
                    /*balance the tree RL-rotation*/
                    Node_bst *temp = (*current)->right;
                    (*current)->right = temp->left;
                    temp->left = NULL;
                    (*current)->right->right = temp;
                    temp = (*current)->right;
                    (*current)->right = NULL;
                    temp->left = (*current);
                    (*current) = temp;
                    bf = height(*current, RIGHT);
                    assert(bf < 1 || bf > -1);
                }
            }
        }
        else if (node->value.i < (*current)->value.i)
        {
            if (!insert_bst(node->type, (void *)&node->value.i,
                            &(*current)->left, &node->data, data_typ))
            {
                free_node(node);
                return 0;
            }

            free_node(node);
            /* check if the tree is balanced*/
            int bf = height(*current, LEFT);
            if (bf > 1 || bf < -1)
            {
                if (bf == 2)
                {
                    /*balance the tree LL-rotation*/
                    Node_bst *temp = (*current)->left;
                    (*current)->left = NULL;
                    temp->right = *current;
                    *current = temp;
                    /*check if the tree now is balanced*/
                    bf = height(*current, LEFT);
                    /* make sure it is balanced */
                    assert(bf < 1 || bf > -1);
                }
                else if (bf == LR_ROT)
                {
                    /*balance the tree LR-rotation */
                    Node_bst *temp = (*current)->left;
                    (*current)->left = temp->right;
                    temp->right = NULL;
                    (*current)->left->left = temp;
                    temp = (*current)->left;
                    (*current)->left = NULL;
                    temp->right = *current;
                    *current = temp;
                    /*check if the tree now is balanced*/
                    bf = height(*current, LEFT);
                    /* make sure it is balanced */
                    assert(bf < 1 || bf > -1);
                }
            }
        }
        else
        {
            free_node(node);
        }
        break;
    case t_l:
        if (node->value.l > (*current)->value.l)
        {
            if (!insert_bst(node->type, (void *)&node->value.l,
                            &(*current)->right, &node->data, data_typ))
            {
                free_node(node);
                return 0;
            }

            free_node(node);
            /* check if the tree is balanced*/
            int bf = height(*current, RIGHT);
            if (bf > 1 || bf < -1)
            {
                if (bf == -2)
                {
                    /*balance the tree RR-rotation*/
                    Node_bst *temp = (*current)->right;
                    (*current)->right = NULL;
                    temp->left = (*current);
                    (*current) = temp;
                    bf = height(*current, RIGHT);
                    assert(bf < 1 || bf > -1); /*make sure the tree now is balanced*/
                }
                else if (bf == RL_ROT)
                {
                    /*balance the tree RL-rotation*/
                    Node_bst *temp = (*current)->right;
                    (*current)->right = temp->left;
                    temp->left = NULL;
                    (*current)->right->right = temp;
                    temp = (*current)->right;
                    (*current)->right = NULL;
                    temp->left = (*current);
                    (*current) = temp;
                    bf = height(*current, RIGHT);
                    assert(bf < 1 || bf > -1);
                }
            }
        }
        else if (node->value.l < (*current)->value.l)
        {
            if (!insert_bst(node->type, (void *)&node->value.l,
                            &(*current)->left, &node->data, data_typ))
            {
                free_node(node);
                return 0;
            }

            free_node(node);
            /* check if the tree is balanced*/
            int bf = height(*current, LEFT);
            if (bf > 1 || bf < -1)
            {
                if (bf == 2)
                {
                    /*balance the tree LL-rotation*/
                    Node_bst *temp = (*current)->left;
                    (*current)->left = NULL;
                    temp->right = *current;
                    *current = temp;
                    /*check if the tree now is balanced*/
                    bf = height(*current, LEFT);
                    /* make sure it is balanced */
                    assert(bf < 1 || bf > -1);
                }
                else if (bf == LR_ROT)
                {
                    /*balance the tree LR-rotation */
                    Node_bst *temp = (*current)->left;
                    (*current)->left = temp->right;
                    temp->right = NULL;
                    (*current)->left->left = temp;
                    temp = (*current)->left;
                    (*current)->left = NULL;
                    temp->right = *current;
                    *current = temp;
                    /*check if the tree now is balanced*/
                    bf = height(*current, LEFT);
                    /* make sure it is balanced */
                    assert(bf < 1 || bf > -1);
                }
            }
        }
        else
        {
            free_node(node);
        }
        break;
    case t_s:
    {
        size_t l = strlen(node->value.s) + 1;
        if (strncmp(node->value.s, (*current)->value.s, l) > 0)
        {
            if (!insert_bst(node->type, (void *)node->value.s, &(*current)->right,
                            &node->data, data_typ))
            {
                free_node(node);
                return 0;
            }

            free_node(node);
            /* check if the tree is balanced*/
            int bf = height(*current, RIGHT);
            if (bf > 1 || bf < -1)
            {
                if (bf == -2)
                {
                    /*balance the tree RR-rotation*/
                    Node_bst *temp = (*current)->right;
                    (*current)->right = NULL;
                    temp->left = (*current);
                    (*current) = temp;
                    bf = height(*current, RIGHT);
                    assert(bf < 1 || bf > -1); /*make sure the tree now is balanced*/
                }
                else if (bf == RL_ROT)
                {
                    /*balance the tree RL-rotation*/
                    Node_bst *temp = (*current)->right;
                    (*current)->right = temp->left;
                    temp->left = NULL;
                    (*current)->right->right = temp;
                    temp = (*current)->right;
                    (*current)->right = NULL;
                    temp->left = (*current);
                    (*current) = temp;
                    bf = height(*current, RIGHT);
                    assert(bf < 1 || bf > -1);
                }
            }
        }
        else if (strncmp(node->value.s, (*current)->value.s, l) < 0)
        {
            if (!insert_bst(node->type, (void *)node->value.s, &(*current)->left,
                            &node->data, data_typ))
            {
                free_node(node);
                return 0;
            }

            free_node(node);
            /* check if the tree is balanced*/
            int bf = height(*current, LEFT);
            if (bf > 1 || bf < -1)
            {
                if (bf == 2)
                {
                    /*balance the tree LL-rotation*/
                    Node_bst *temp = (*current)->left;
                    (*current)->left = NULL;
                    temp->right = *current;
                    *current = temp;
                    /*check if the tree now is balanced*/
                    bf = height(*current, LEFT);
                    /* make sure it is balanced */
                    assert(bf < 1 || bf > -1);
                }
                else if (bf == LR_ROT)
                {
                    /*balance the tree LR-rotation */
                    Node_bst *temp = (*current)->left;
                    (*current)->left = temp->right;
                    temp->right = NULL;
                    (*current)->left->left = temp;
                    temp = (*current)->left;
                    (*current)->left = NULL;
                    temp->right = *current;
                    *current = temp;
                    /*check if the tree now is balanced*/
                    bf = height(*current, LEFT);
                    /* make sure it is balanced */
                    assert(bf < 1 || bf > -1);
                }
            }
        }
        else
        {
            free_node(node);
        }
        break;
    }
    case t_v:
    {
        if (node->value.ptr > (*current)->value.ptr)
        {
            if (!insert_bst(node->type, (void *)node->value.ptr,
                            &(*current)->right, &node->data, data_typ))
            {
                free_node(node);
                return 0;
            }

            free_node(node);

            /* check if the tree is balanced*/
            int bf = height(*current, RIGHT);
            if (bf > 1 || bf < -1)
            {
                if (bf == -2)
                {
                    /*balance the tree RR-rotation*/
                    Node_bst *temp = (*current)->right;
                    (*current)->right = NULL;
                    temp->left = (*current);
                    (*current) = temp;
                    bf = height(*current, RIGHT);
                    assert(bf < 1 || bf > -1); /*make sure the tree now is balanced*/
                }
                else if (bf == RL_ROT)
                {
                    /*balance the tree RL-rotation*/
                    Node_bst *temp = (*current)->right;
                    (*current)->right = temp->left;
                    temp->left = NULL;
                    (*current)->right->right = temp;
                    temp = (*current)->right;
                    (*current)->right = NULL;
                    temp->left = (*current);
                    (*current) = temp;
                    bf = height(*current, RIGHT);
                    assert(bf < 1 || bf > -1);
                }
            }
        }
        else if (node->value.ptr < (*current)->value.ptr)
        {
            if (!insert_bst(node->type, (void *)node->value.ptr,
                            &(*current)->left, &node->data, data_typ))
            {
                free_node(node);
                return 0;
            }

            free_node(node);
            /* check if the tree is balanced*/
            int bf = height(*current, LEFT);
            if (bf > 1 || bf < -1)
            {
                if (bf == 2)
                {
                    /*balance the tree LL-rotation*/
                    Node_bst *temp = (*current)->left;
                    (*current)->left = NULL;
                    temp->right = *current;
                    *current = temp;
                    /*check if the tree now is balanced*/
                    bf = height(*current, LEFT);
                    /* make sure it is balanced */
                    assert(bf < 1 || bf > -1);
                }
                else if (bf == LR_ROT)
                {
                    /*balance the tree LR-rotation */
                    Node_bst *temp = (*current)->left;
                    (*current)->left = temp->right;
                    temp->right = NULL;
                    (*current)->left->left = temp;
                    temp = (*current)->left;
                    (*current)->left = NULL;
                    temp->right = *current;
                    *current = temp;
                    /*check if the tree now is balanced*/
                    bf = height(*current, LEFT);
                    /* make sure it is balanced */
                    assert(bf < 1 || bf > -1);
                }
            }
        }
        else
        {
            free_node(node);
        }
        break;
    }
    default:
        printf("unkown type for BST.\n");
        return 0;
    }

    return 1;
}

unsigned char find(Types type, void *value, Node_bst **current, void **data, Types data_typ)
{
    if (!*current)
        return 0;

    if ((*current)->type != type)
    {
        printf("node type not valid.\n");
        return 0;
    }

    switch (type)
    {
    case t_i:
        if ((*(int *)value) > (*current)->value.i)
        {
            if (!find(type, (void *)value, &(*current)->right, data, data_typ))
                return 0;
        }
        else if ((*(int *)value) < (*current)->value.i)
        {
            if (!find(type, (void *)value, &(*current)->left, data, data_typ))
                return 0;
        }
        else
        {
            switch (data_typ)
            {
            case rec_t:
                *data = (struct Record_f *)(*current)->data;
                break;
            case t_i:
                *data = (int *)(*current)->data;
                break;
            case t_l:
                *data = (long *)(*current)->data;
                break;
            case t_f:
                *data = (float *)(*current)->data;
                break;
            case t_d:
                *data = (double *)(*current)->data;
                break;
            case t_s:
                *data = (char *)(*current)->data;
                break;
            case t_NL:
                break;
            default:
                printf("unknown type for BST.\n");
                return 0;
            }
            return 1;
        }
        break;
    case t_l:
        if ((*(long *)value) > (*current)->value.l)
        {
            if (!find(type, (void *)value, &(*current)->right, data, data_typ))
                return 0;
        }
        else if ((*(long *)value) < (*current)->value.l)
        {
            if (!find(type, (void *)&value, &(*current)->left, data, data_typ))
                return 0;
        }
        else
        {
            switch (data_typ)
            {
            case rec_t:
                *data = (struct Record_f *)(*current)->data;
                break;
            case t_i:
                *data = (int *)(*current)->data;
                break;
            case t_l:
                *data = (long *)(*current)->data;
                break;
            case t_f:
                *data = (float *)(*current)->data;
                break;
            case t_d:
                *data = (double *)(*current)->data;
                break;
            case t_s:
                *data = (char *)(*current)->data;
                break;
            default:
                printf("unknown type for BST.\n");
                return 0;
            }

            return 1;
        }
        break;
    case t_s:
    {
        size_t l = strlen((char *)value) + 1;
        if (strncmp((char *)value, (*current)->value.s, l) > 0)
        {
            if (!find(type, (void *)value, &(*current)->right, data, data_typ))
                return 0;
        }
        else if (strncmp((char *)value, (*current)->value.s, l) < 0)
        {
            if (!find(type, (void *)value, &(*current)->left, data, data_typ))
                return 0;
        }
        else
        {
            switch (data_typ)
            {
            case rec_t:
                *data = (struct Record_f *)(*current)->data;
                break;
            case t_i:
                *data = (int *)(*current)->data;
                break;
            case t_l:
                *data = (long *)(*current)->data;
                break;
            case t_f:
                *data = (float *)(*current)->data;
                break;
            case t_d:
                *data = (double *)(*current)->data;
                break;
            case t_s:
                *data = (char *)(*current)->data;
                break;
            default:
                printf("unknown type for BST.\n");
                return 0;
            }

            return 1;
        }
        break;
    }
    case t_v:
        if (value > (*current)->value.ptr)
        {
            if (!find(type, (void *)value, &(*current)->right, data, data_typ))
                return 0;
        }
        else if (value < (*current)->value.ptr)
        {
            if (!find(type, (void *)value, &(*current)->left, data, data_typ))
                return 0;
        }
        else
        {
            switch (data_typ)
            {
            case rec_t:
                *data = (struct Record_f *)(*current)->data;
                break;
            case t_i:
                *data = (int *)(*current)->data;
                break;
            case t_l:
                *data = (long *)(*current)->data;
                break;
            case t_f:
                *data = (float *)(*current)->data;
                break;
            case t_d:
                *data = (double *)(*current)->data;
                break;
            case t_s:
                *data = (char *)(*current)->data;
                break;
            default:
                printf("unknown type for BST.\n");
                return 0;
            }
            return 1;
        }
        break;

    default:
        printf("unkown type for BST.\n");
        return 0;
    }

    return 1;
}

void free_node(Node_bst *node)
{
    if (node->type == t_s)
        free(node->value.s);

    if (node->data)
        free(node->data);

    if (node->left)
        free_node(node->left);

    if (node->right)
        free_node(node->right);

    if (node)
        free(node);

    return;
}

void free_BST(BST *binary_st)
{
    free_node(binary_st->root);
}

/* Branch is coming from the root node, here the root node is consider the first node of
    a group of 3 nodes */
int height(Node_bst *node, int branch)
{
    if (!node)
        return -1;

    if (node)
    {
        if (!node->left && !node->right)
            return 0;
    }

    if (node->left && node->right)
        return 0;

    if (node->right && !node->left)
    {
        int bf_rs = height(node->right, branch);
        if (branch == RIGHT && bf_rs == 1)
            return RL_ROT;

        return bf_rs - 1;
    }
    else
    {
        int bf_ls = height(node->left, branch);
        if (branch == LEFT && bf_ls == -1)
            return LR_ROT;

        return bf_ls + 1;
    }
}
