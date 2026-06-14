#pragma once

#include "erl_nif.h"

namespace faiss_nif {

// Atom declarations
extern ERL_NIF_TERM ATOM_OK;
extern ERL_NIF_TERM ATOM_ERROR;
extern ERL_NIF_TERM ATOM_TRUE;
extern ERL_NIF_TERM ATOM_FALSE;
extern ERL_NIF_TERM ATOM_L2;
extern ERL_NIF_TERM ATOM_INNER_PRODUCT;
extern ERL_NIF_TERM ATOM_BADARG;
extern ERL_NIF_TERM ATOM_NOT_TRAINED;

// Initialize atoms - call from on_load
int init_atoms(ErlNifEnv* env);

// Helper to create error tuple
ERL_NIF_TERM make_error(ErlNifEnv* env, const char* reason);
ERL_NIF_TERM make_error(ErlNifEnv* env, ERL_NIF_TERM reason);

}  // namespace faiss_nif
