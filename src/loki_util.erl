-module(loki_util).

-export([to_list/1,
         to_atom/1]).

%% @doc Try and convert given data type to list. Invalid data types with throw
%% a pattern match exception
-spec to_list(list() | binary() | atom() | integer()) -> list().
to_list(L) when is_list(L)    -> L;
to_list(B) when is_binary(B)  -> binary_to_list(B);
to_list(A) when is_atom(A)    -> atom_to_list(A);
to_list(I) when is_integer(I) -> integer_to_list(I);
to_list(F) when is_float(F)   -> float_to_list(F).

%% @doc Try and convert given data type to atom. Invalid data types with throw
%% a pattern match exception
-spec to_atom(atom() | string() | integer() | binary()) -> atom().
to_atom(A) when is_atom(A)    -> A;
to_atom(L) when is_list(L)    -> list_to_atom(L);
to_atom(I) when is_integer(I) -> list_to_atom(integer_to_list(I));
to_atom(B) when is_binary(B)  -> binary_to_atom(B, utf8).
