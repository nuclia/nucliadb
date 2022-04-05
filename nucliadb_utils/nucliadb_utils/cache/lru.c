/*
 * Copyright (C) 2021 Bosutech XXI S.L.
 *
 * nucliadb is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at info@nuclia.com.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
#include <Python.h>
#include <stdint.h>
#include <sys/time.h>

/*
 * This package is based on a fork of https://github.com/amitdev/lru-dict
 * We added the feature of tracking object sizes from python, without calculating
 * them. To use this feature, instead os relaying on the dict syntax,
 * you must explicity set the values, with the new provided method:
 * for eg:
 *
 * l = LRU(200*1024*1024)
 * l.set('key', 'val', 3)
 *
 *
 * This is a simple implementation of LRU Dict that uses a Python dict and an associated doubly linked
 * list to keep track of recently inserted/accessed items.
 *
 * Dict will store: key -> Node mapping, where Node is a linked list node.
 * The Node itself will contain the value as well as the key.
 *
 * For eg:
 *
 * >>> l = LRU(2)
 * >>> l[0] = 'foo'
 * >>> l[1] = 'bar'
 *
 * can be visualised as:
 *
 *             ---+--(hash(0)--+--hash(1)--+
 * self->dict  ...|            |           |
 *             ---+-----|------+---------|-+
 *                      |                |
 *                +-----v------+   +-----v------+
 * self->first--->|<'foo'>, <0>|-->|<'bar'>, <1>|<---self->last
 *             +--|            |<--|            |--+
 *             |  +------------+   +------------+  |
 *             v                                   v
 *           NULL                                 NULL
 *
 *  The invariant is to maintain the list to reflect the LRU order of items in the dict.
 *  self->first will point to the MRU item and self-last to LRU item. Size of list will not
 *  grow beyond size of LRU dict.
 *
 */

#ifndef Py_TYPE
 #define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#define GET_NODE(d, key) (Node *) Py_TYPE(d)->tp_as_mapping->mp_subscript((d), (key))
#define PUT_NODE(d, key, node) Py_TYPE(d)->tp_as_mapping->mp_ass_subscript((d), (key), ((PyObject *)node))

/* If someone figures out how to enable debug builds with setuptools, you can delete this */
#if 0
#undef assert
#define str(s) #s
#define assert(v) \
  do {                                                                                \
    if (!(v)) {                                                                       \
      fprintf(stderr, "Assertion failed: %s on %s:%d\n",                              \
              str(v), __FILE__, __LINE__);                                            \
      fflush(stderr);                                                                 \
      abort();                                                                        \
    }                                                                                 \
  } while(0)
#endif

typedef struct _Node {
    PyObject_HEAD
    PyObject * value;
    PyObject * key;
    Py_ssize_t size;   /* This tracks the current object size */
    struct _Node * prev;
    struct _Node * next;
} Node;

static void
node_dealloc(Node* self)
{
    Py_DECREF(self->key);
    Py_DECREF(self->value);
    assert(self->prev == NULL);
    assert(self->next == NULL);
    PyObject_Del((PyObject*)self);
}

static PyObject*
node_repr(Node* self)
{
    return PyObject_Repr(self->value);
}

static PyTypeObject NodeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "lru.Node",              /* tp_name */
    sizeof(Node),            /* tp_basicsize */
    0,                       /* tp_itemsize */
    (destructor)node_dealloc,/* tp_dealloc */
    0,                       /* tp_print */
    0,                       /* tp_getattr */
    0,                       /* tp_setattr */
    0,                       /* tp_compare */
    (reprfunc)node_repr,     /* tp_repr */
    0,                       /* tp_as_number */
    0,                       /* tp_as_sequence */
    0,                       /* tp_as_mapping */
    0,                       /* tp_hash */
    0,                       /* tp_call */
    0,                       /* tp_str */
    0,                       /* tp_getattro */
    0,                       /* tp_setattro */
    0,                       /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,      /* tp_flags */
    "Linked List Node",      /* tp_doc */
    0,                       /* tp_traverse */
    0,                       /* tp_clear */
    0,                       /* tp_richcompare */
    0,                       /* tp_weaklistoffset */
    0,                       /* tp_iter */
    0,                       /* tp_iternext */
    0,                       /* tp_methods */
    0,                       /* tp_members */
    0,                       /* tp_getset */
    0,                       /* tp_base */
    0,                       /* tp_dict */
    0,                       /* tp_descr_get */
    0,                       /* tp_descr_set */
    0,                       /* tp_dictoffset */
    0,                       /* tp_init */
    0,                       /* tp_alloc */
    0,                       /* tp_new */
};

typedef struct {
    PyObject_HEAD
    PyObject * dict;
    Node * first;
    Node * last;
    Py_ssize_t memory;  /* tracks the current memory used */
    Py_ssize_t size;
    Py_ssize_t hits;
    Py_ssize_t clean;
    Py_ssize_t misses;
    PyObject *callback;
} LRU;


static PyObject *
set_callback(LRU *self, PyObject *args)
{
    PyObject *result = NULL;
    PyObject *temp;

    if (PyArg_ParseTuple(args, "O:set_callback", &temp)) {
        if (temp == Py_None) {
            Py_XDECREF(self->callback);
            self->callback = NULL;
        } else if (!PyCallable_Check(temp)) {
            PyErr_SetString(PyExc_TypeError, "parameter must be callable");
            return NULL;
        } else {
            Py_XINCREF(temp);         /* Add a reference to new callback */
            Py_XDECREF(self->callback);  /* Dispose of previous callback */
            self->callback = temp;       /* Remember new callback */
        }
        Py_RETURN_NONE;
    }
    return result;
}

static void
lru_remove_node(LRU *self, Node* node)
{
    if (self->first == node) {
        self->first = node->next;
    }
    if (self->last == node) {
        self->last = node->prev;
    }
    if (node->prev) {
        node->prev->next = node->next;
    }
    if (node->next) {
        node->next->prev = node->prev;
    }
    node->next = node->prev = NULL;
}

static void
lru_add_node_at_head(LRU *self, Node* node)
{
    node->prev = NULL;
    if (!self->first) {
        self->first = self->last = node;
        node->next = NULL;
    } else {
        node->next = self->first;
        if (node->next) {
            node->next->prev = node;
        }
        self->first = node;
    }
}

static void
lru_delete_last(LRU *self)
{
    PyObject *arglist;
    PyObject *result;
    Node* n = self->last;

    if (!self->last)
        return;

    if (self->callback) {

        arglist = Py_BuildValue("OO", n->key, n->value);
        result = PyObject_CallObject(self->callback, arglist);
        Py_XDECREF(result);
        Py_DECREF(arglist);
    }

    lru_remove_node(self, n);
    PUT_NODE(self->dict, n->key, NULL);
}

static Py_ssize_t
lru_length(LRU *self)
{
    return PyDict_Size(self->dict);
}

static PyObject *
LRU_memory(LRU *self)
{
    return Py_BuildValue("l", self->memory);
}

static PyObject *
LRU_contains_key(LRU *self, PyObject *key)
{
    if (PyDict_Contains(self->dict, key)) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject *
LRU_contains(LRU *self, PyObject *args)
{
    PyObject *key;
    if (!PyArg_ParseTuple(args, "O", &key))
        return NULL;
    return LRU_contains_key(self, key);
}

static int
LRU_seq_contains(LRU *self, PyObject *key)
{
    return PyDict_Contains(self->dict, key);
}

static PyObject *
lru_subscript(LRU *self, register PyObject *key)
{
    Node *node = GET_NODE(self->dict, key);
    if (!node) {
        self->misses++;
        return NULL;
    }

    assert(PyObject_TypeCheck(node, &NodeType));

    /* We don't need to move the node when it's already self->first. */
    if (node != self->first) {
        lru_remove_node(self, node);
        lru_add_node_at_head(self, node);
    }

    self->hits++;
    Py_INCREF(node->value);
    Py_DECREF(node);
    return node->value;
}

static PyObject *
LRU_get(LRU *self, PyObject *args)
{
    PyObject *key;
    PyObject *instead = NULL;
    PyObject *result;

    if (!PyArg_ParseTuple(args, "O|O", &key, &instead))
        return NULL;

    result = lru_subscript(self, key);
    PyErr_Clear();  /* GET_NODE sets an exception on miss. Shut it up. */
    if (result)
        return result;

    if (!instead) {
        Py_RETURN_NONE;
    }

    Py_INCREF(instead);
    return instead;
}


void lru_vacuum(LRU *self) {
    // vacuuum
    while(self->memory > self->size) {
        self->memory -= self->last->size;
        lru_delete_last(self);
        self->clean += 1;
    }
}


static int
_lru_ass_sub(LRU *self, PyObject *key, PyObject *value, Py_ssize_t memory)
{

    int res = 0;
    /* do nothing if provided val is greater in size than the desired cache */
    if (memory > self->size) {
        return res;
    }

    Node *node = GET_NODE(self->dict, key);
    PyErr_Clear();  /* GET_NODE sets an exception on miss. Shut it up. */
    if (value) {
        if (node) {
            Py_INCREF(value);
            Py_DECREF(node->value);
            node->value = value;
            self->memory = self->memory - node->size + memory;
            node->size = memory;
            lru_remove_node(self, node);
            lru_add_node_at_head(self, node);

            res = 0;
        } else {
            node = PyObject_NEW(Node, &NodeType);
            node->key = key;
            node->value = value;
            node->next = node->prev = NULL;
            node->size = memory;
            self->memory += memory;

            Py_INCREF(key);
            Py_INCREF(value);

            res = PUT_NODE(self->dict, key, node);
            lru_add_node_at_head(self, node);
        }

        lru_vacuum(self);

    } else {
        res = PUT_NODE(self->dict, key, NULL);
        if (res == 0) {
            assert(node && PyObject_TypeCheck(node, &NodeType));
            self->memory -= node->size;
            lru_remove_node(self, node);

        }
    }

    Py_XDECREF(node);
    return res;
}


static int lru_ass_sub(LRU *self, PyObject *key, PyObject *value)
{
    /* If we are using the dict syntax l['a'] we can track size of values
       and we track them as 0 */
    return _lru_ass_sub(self, key, value, 0);
}

static PyObject *
LRU_set(LRU *self, PyObject *args) {

    PyObject *key, *value;
    Py_ssize_t mem;

    if (!PyArg_ParseTuple(args, "OOl", &key, &value, &mem))
            return NULL;
    _lru_ass_sub(self, key, value, mem);
    Py_RETURN_NONE;
}

static PyMappingMethods LRU_as_mapping = {
    (lenfunc)lru_length,        /*mp_length*/
    (binaryfunc)lru_subscript,  /*mp_subscript*/
    (objobjargproc)lru_ass_sub, /*mp_ass_subscript*/
};

static PyObject *
collect(LRU *self, PyObject * (*getterfunc)(Node *))
{
    register PyObject *v;
    Node *curr;
    int i;
    v = PyList_New(lru_length(self));
    if (v == NULL)
        return NULL;
    curr = self->first;
    i = 0;

    while (curr) {
        PyList_SET_ITEM(v, i++, getterfunc(curr));
        curr = curr->next;
    }
    assert(i == lru_length(self));
    return v;
}

static PyObject *
get_key(Node *node)
{
    Py_INCREF(node->key);
    return node->key;
}

static PyObject *
LRU_update(LRU *self, PyObject *args, PyObject *kwargs)
{
	PyObject *key, *value;
	PyObject *arg = NULL;
	Py_ssize_t pos = 0;

	if ((PyArg_ParseTuple(args, "|O", &arg))) {
		if (arg && PyDict_Check(arg)) {
			while (PyDict_Next(arg, &pos, &key, &value))
				_lru_ass_sub(self, key, value, 0);
		}
	}

	if (kwargs != NULL && PyDict_Check(kwargs)) {
		while (PyDict_Next(kwargs, &pos, &key, &value))
			_lru_ass_sub(self, key, value, 0);
	}

	Py_RETURN_NONE;
}

static PyObject *
LRU_peek_first_item(LRU *self)
{
    if (self->first) {
        PyObject *tuple = PyTuple_New(2);
        Py_INCREF(self->first->key);
        PyTuple_SET_ITEM(tuple, 0, self->first->key);
        Py_INCREF(self->first->value);
        PyTuple_SET_ITEM(tuple, 1, self->first->value);
        return tuple;
    }
    else Py_RETURN_NONE;
}

static PyObject *
LRU_peek_last_item(LRU *self)
{
    if (self->last) {
        PyObject *tuple = PyTuple_New(2);
        Py_INCREF(self->last->key);
        PyTuple_SET_ITEM(tuple, 0, self->last->key);
        Py_INCREF(self->last->value);
        PyTuple_SET_ITEM(tuple, 1, self->last->value);
        return tuple;
    }
    else Py_RETURN_NONE;
}

static PyObject *
LRU_keys(LRU *self) {
    return collect(self, get_key);
}

static PyObject *
get_value(Node *node)
{
    Py_INCREF(node->value);
    return node->value;
}

static PyObject *
LRU_values(LRU *self)
{
    return collect(self, get_value);
}

static PyObject *
LRU_set_callback(LRU *self, PyObject *args)
{
    return set_callback(self, args);
}

static PyObject *
get_item(Node *node)
{
    PyObject *tuple = PyTuple_New(2);
    Py_INCREF(node->key);
    PyTuple_SET_ITEM(tuple, 0, node->key);
    Py_INCREF(node->value);
    PyTuple_SET_ITEM(tuple, 1, node->value);
    return tuple;
}

static PyObject *
LRU_items(LRU *self)
{
    return collect(self, get_item);
}

static PyObject *
LRU_set_size(LRU *self, PyObject *args, PyObject *kwds)
{
    Py_ssize_t newSize;
    if (!PyArg_ParseTuple(args, "l", &newSize)) {
        return NULL;
    }
    if (newSize <= 0) {
        PyErr_SetString(PyExc_ValueError, "Size should be a positive number");
        return NULL;
    }
    while (lru_length(self) > newSize) {
        lru_delete_last(self);
    }
    self->size = newSize;
    Py_RETURN_NONE;
}

static PyObject *
LRU_clear(LRU *self)
{
    Node *c = self->first;

    while (c) {
        Node* n = c;
        c = c->next;
        lru_remove_node(self, n);
    }
    PyDict_Clear(self->dict);

    self->hits = 0;
    self->misses = 0;
    self->memory = 0;
    Py_RETURN_NONE;
}


static PyObject *
LRU_get_size(LRU *self)
{
    return Py_BuildValue("i", self->size);
}

static PyObject *
LRU_get_stats(LRU *self)
{
    return Py_BuildValue("nnn", self->hits, self->misses, self->clean);
}


/* Hack to implement "key in lru" */
static PySequenceMethods lru_as_sequence = {
    0,                             /* sq_length */
    0,                             /* sq_concat */
    0,                             /* sq_repeat */
    0,                             /* sq_item */
    0,                             /* sq_slice */
    0,                             /* sq_ass_item */
    0,                             /* sq_ass_slice */
    (objobjproc) LRU_seq_contains, /* sq_contains */
    0,                             /* sq_inplace_concat */
    0,                             /* sq_inplace_repeat */
};

static PyMethodDef LRU_methods[] = {
    {"__contains__", (PyCFunction)LRU_contains_key, METH_O | METH_COEXIST,
                    PyDoc_STR("L.__contains__(key) -> Check if key is there in L")},
    {"keys", (PyCFunction)LRU_keys, METH_NOARGS,
                    PyDoc_STR("L.keys() -> list of L's keys in MRU order")},
    {"values", (PyCFunction)LRU_values, METH_NOARGS,
                    PyDoc_STR("L.values() -> list of L's values in MRU order")},
    {"items", (PyCFunction)LRU_items, METH_NOARGS,
                    PyDoc_STR("L.items() -> list of L's items (key,value) in MRU order")},
    {"has_key",	(PyCFunction)LRU_contains, METH_VARARGS,
                    PyDoc_STR("L.has_key(key) -> Check if key is there in L")},
    {"get",	(PyCFunction)LRU_get, METH_VARARGS,
                    PyDoc_STR("L.get(key, instead) -> If L has key return its value, otherwise instead")},
    {"set",	(PyCFunction)LRU_set, METH_VARARGS,
                        PyDoc_STR("L.set(key, value, size)")},
    {"set_size", (PyCFunction)LRU_set_size, METH_VARARGS,
                    PyDoc_STR("L.set_size() -> set size of LRU")},
    {"get_size", (PyCFunction)LRU_get_size, METH_NOARGS,
                    PyDoc_STR("L.get_size() -> get size of LRU")},
    {"get_memory", (PyCFunction)LRU_memory, METH_NOARGS,
                        PyDoc_STR("L.memory() -> get memory used")},
    {"clear", (PyCFunction)LRU_clear, METH_NOARGS,
                    PyDoc_STR("L.clear() -> clear LRU")},
    {"get_stats", (PyCFunction)LRU_get_stats, METH_NOARGS,
                    PyDoc_STR("L.get_stats() -> returns a tuple with cache hits and misses")},
    {"peek_first_item", (PyCFunction)LRU_peek_first_item, METH_NOARGS,
                    PyDoc_STR("L.peek_first_item() -> returns the MRU item (key,value) without changing key order")},
    {"peek_last_item", (PyCFunction)LRU_peek_last_item, METH_NOARGS,
                    PyDoc_STR("L.peek_last_item() -> returns the LRU item (key,value) without changing key order")},
    {"update", (PyCFunction)LRU_update, METH_VARARGS | METH_KEYWORDS,
                    PyDoc_STR("L.update() -> update value for key in LRU")},
    {"set_callback", (PyCFunction)LRU_set_callback, METH_VARARGS,
                    PyDoc_STR("L.set_callback(callback) -> set a callback to call when an item is evicted.")},
    {NULL,	NULL},
};

static PyObject*
LRU_repr(LRU* self)
{
    return PyObject_Repr(self->dict);
}

static int
LRU_init(LRU *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"size", "callback", NULL};
    PyObject *callback = NULL;
    self->callback = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "l|O", kwlist, &self->size, &callback)) {
        return -1;
    }

    if (callback && callback != Py_None) {
        if (!PyCallable_Check(callback)) {
            PyErr_SetString(PyExc_TypeError, "parameter must be callable");
            return -1;
        }
        Py_XINCREF(callback);
        self->callback = callback;
    }

    if ((Py_ssize_t)self->size <= 0) {
        PyErr_SetString(PyExc_ValueError, "Size should be a positive number");
        return -1;
    }
    self->dict = PyDict_New();
    self->first = self->last = NULL;
    self->hits = 0;
    self->misses = 0;
    self->memory = 0;
    self->clean = 0;
    return 0;
}

static void
LRU_dealloc(LRU *self)
{
    if (self->dict) {
        LRU_clear(self);
        Py_DECREF(self->dict);
        Py_XDECREF(self->callback);
    }
    PyObject_Del((PyObject*)self);
}

PyDoc_STRVAR(lru_doc,
"LRU(size, callback=None) -> new LRU dict that can store up to size elements\n"
"An LRU dict behaves like a standard dict, except that it stores only fixed\n"
"set of elements. Once the size overflows, it evicts least recently used\n"
"items.  If a callback is set it will call the callback with the evicted key\n"
" and item.\n\n"
"Eg:\n"
">>> l = LRU(3)\n"
">>> for i in range(5):\n"
">>>   l[i] = str(i)\n"
">>> l.keys()\n"
"[2,3,4]\n\n"
"Note: An LRU(n) can be thought of as a dict that will have the most\n"
"recently accessed n items.\n");

static PyTypeObject LRUType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "lru.LRU",               /* tp_name */
    sizeof(LRU),             /* tp_basicsize */
    0,                       /* tp_itemsize */
    (destructor)LRU_dealloc, /* tp_dealloc */
    0,                       /* tp_print */
    0,                       /* tp_getattr */
    0,                       /* tp_setattr */
    0,                       /* tp_compare */
    (reprfunc)LRU_repr,      /* tp_repr */
    0,                       /* tp_as_number */
    &lru_as_sequence,        /* tp_as_sequence */
    &LRU_as_mapping,         /* tp_as_mapping */
    0,                       /* tp_hash */
    0,                       /* tp_call */
    0,                       /* tp_str */
    0,                       /* tp_getattro */
    0,                       /* tp_setattro */
    0,                       /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,      /* tp_flags */
    lru_doc,                 /* tp_doc */
    0,                       /* tp_traverse */
    0,                       /* tp_clear */
    0,                       /* tp_richcompare */
    0,                       /* tp_weaklistoffset */
    0,                       /* tp_iter */
    0,                       /* tp_iternext */
    LRU_methods,             /* tp_methods */
    0,                       /* tp_members */
    0,                       /* tp_getset */
    0,                       /* tp_base */
    0,                       /* tp_dict */
    0,                       /* tp_descr_get */
    0,                       /* tp_descr_set */
    0,                       /* tp_dictoffset */
    (initproc)LRU_init,      /* tp_init */
    0,                       /* tp_alloc */
    0,                       /* tp_new */
};

#if PY_MAJOR_VERSION >= 3
  static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "lru",            /* m_name */
    lru_doc,          /* m_doc */
    -1,               /* m_size */
    NULL,             /* m_methods */
    NULL,             /* m_reload */
    NULL,             /* m_traverse */
    NULL,             /* m_clear */
    NULL,             /* m_free */
  };
#endif

static PyObject *
moduleinit(void)
{
    PyObject *m;

    NodeType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&NodeType) < 0)
        return NULL;

    LRUType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&LRUType) < 0)
        return NULL;

    #if PY_MAJOR_VERSION >= 3
        m = PyModule_Create(&moduledef);
    #else
        m = Py_InitModule3("lru", NULL, lru_doc);
    #endif

    if (m == NULL)
        return NULL;

    Py_INCREF(&NodeType);
    Py_INCREF(&LRUType);
    PyModule_AddObject(m, "LRU", (PyObject *) &LRUType);

    return m;
}

#if PY_MAJOR_VERSION < 3
    PyMODINIT_FUNC
    initlru(void)
    {
        moduleinit();
    }
#else
    PyMODINIT_FUNC
    PyInit_lru(void)
    {
        return moduleinit();
    }
#endif
