-module(loki_util).

-export([to_list/1]).

%% @doc Try and convert given data type to list. Invalid data types with throw
%% a pattern match exception
to_list(L) when is_list(L)    -> L;
to_list(B) when is_binary(B)  -> binary_to_list(B);
to_list(A) when is_atom(A)    -> atom_to_list(A);
to_list(I) when is_integer(I) -> integer_to_list(I);
to_list(F) when is_float(F)   -> float_to_list(F).
