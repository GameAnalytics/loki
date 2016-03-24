%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2014. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%

%%
%% This file contains the tab2file and file2tab from erlang 18.
%% In 18 the sync option was added to tab2file and file2tab restores
%% read_concurrency and write_concurrency options correctly.
%%
%% These two files are backported via this module so that we can do restore
%% from disk correctly while running r16.
%%
%% The reason is that r16 gives stream much better and consistent performance,
%% the reason for the bad performance on 18 is unknown.
%%
%% The idea for this comes from:
%% https://github.com/Feuerlabs/kvdb/blob/master/src/kvdb_ets_dumper.erl
%%

-module(ets_backports).

%% Interface to the Term store BIF's
%% ets == Erlang Term Store

-export([file2tab/1,
         file2tab/2,
         tab2file/2,
         tab2file/3,
         tabfile_info/1]).

%% Dump a table to a file using the disk_log facility

%% Options := [Option]
%% Option := {extended_info,[ExtInfo]}
%% ExtInfo := object_count | md5sum

-define(MAJOR_F2T_VERSION,1).
-define(MINOR_F2T_VERSION,0).

-record(filetab_options,
        {
         object_count = false :: boolean(),
         md5sum       = false :: boolean(),
         sync         = false :: boolean()
        }).

-spec tab2file(Tab, Filename) -> 'ok' | {'error', Reason} when
      Tab :: ets:tab(),
      Filename :: file:name(),
      Reason :: term().

tab2file(Tab, File) ->
    tab2file(Tab, File, []).

-spec tab2file(Tab, Filename, Options) -> 'ok' | {'error', Reason} when
      Tab :: ets:tab(),
      Filename :: file:name(),
      Options :: [Option],
      Option :: {'extended_info', [ExtInfo]} | {'sync', boolean()},
      ExtInfo :: 'md5sum' | 'object_count',
      Reason :: term().

tab2file(Tab, File, Options) ->
    try
        {ok, FtOptions} = parse_ft_options(Options),
        _ = file:delete(File),
        case file:read_file_info(File) of
            {error, enoent} -> ok;
            _ -> throw(eaccess)
        end,
        Name = make_ref(),
        case disk_log:open([{name, Name}, {file, File}]) of
            {ok, Name} ->
                ok;
            {error, Reason} ->
                throw(Reason)
        end,
        try
            Info0 = case ets:info(Tab) of
                        undefined ->
                            %% erlang:error(badarg, [Tab, File, Options]);
                            throw(badtab);
                        I ->
                            I
                    end,
            Info = [list_to_tuple(Info0 ++
                                  [{major_version,?MAJOR_F2T_VERSION},
                                   {minor_version,?MINOR_F2T_VERSION},
                                   {extended_info,
                                    ft_options_to_list(FtOptions)}])],
            {LogFun, InitState} =
            case FtOptions#filetab_options.md5sum of
                true ->
                    {fun(Oldstate,Termlist) ->
                             {NewState,BinList} =
                             md5terms(Oldstate,Termlist),
                             case disk_log:blog_terms(Name,BinList) of
                                 ok -> NewState;
                                 {error, Reason2} -> throw(Reason2)
                             end
                     end,
                     erlang:md5_init()};
                false ->
                    {fun(_,Termlist) ->
                             case disk_log:log_terms(Name,Termlist) of
                                 ok -> true;
                                 {error, Reason2} -> throw(Reason2)
                             end
                     end,
                     true}
            end,
            ets:safe_fixtable(Tab,true),
            {NewState1,Num} = try
                                  NewState = LogFun(InitState,Info),
                                  dump_file(
                                    ets:select(Tab,[{'_',[],['$_']}],100),
                                    LogFun, NewState, 0)
                              after
                                  (catch ets:safe_fixtable(Tab,false))
                              end,
            EndInfo =
            case  FtOptions#filetab_options.object_count of
                true ->
                    [{count,Num}];
                false ->
                    []
            end ++
            case  FtOptions#filetab_options.md5sum of
                true ->
                    [{md5,erlang:md5_final(NewState1)}];
                false ->
                    []
            end,
            case EndInfo of
                [] ->
                    ok;
                List ->
                    LogFun(NewState1,[['$end_of_table',List]])
            end,
            case FtOptions#filetab_options.sync of
                true ->
                    case disk_log:sync(Name) of
                        ok -> ok;
                        {error, Reason2} -> throw(Reason2)
                    end;
                false ->
                    ok
            end,
            disk_log:close(Name)
        catch
            throw:TReason ->
                _ = disk_log:close(Name),
                _ = file:delete(File),
                throw(TReason);
            exit:ExReason ->
                _ = disk_log:close(Name),
                _ = file:delete(File),
                exit(ExReason);
            error:ErReason ->
                _ = disk_log:close(Name),
                _ = file:delete(File),
                erlang:raise(error,ErReason,erlang:get_stacktrace())
        end
    catch
        throw:TReason2 ->
            {error,TReason2};
        exit:ExReason2 ->
            {error,ExReason2}
    end.

dump_file('$end_of_table', _LogFun, State, Num) ->
    {State,Num};
dump_file({Terms, Context}, LogFun, State, Num) ->
    Count = length(Terms),
    NewState = LogFun(State, Terms),
    dump_file(ets:select(Context), LogFun, NewState, Num + Count).

ft_options_to_list(#filetab_options{md5sum = MD5, object_count = PS}) ->
    case PS of
        true ->
            [object_count];
        _ ->
            []
    end ++
    case MD5 of
        true ->
            [md5sum];
        _ ->
            []
    end.

md5terms(State, []) ->
    {State, []};
md5terms(State, [H|T]) ->
    B = term_to_binary(H),
    NewState = erlang:md5_update(State, B),
    {FinState, TL} = md5terms(NewState, T),
    {FinState, [B|TL]}.

parse_ft_options(Options) when is_list(Options) ->
    {ok, parse_ft_options(Options, #filetab_options{}, false)}.

parse_ft_options([], FtOpt, _) ->
    FtOpt;
parse_ft_options([{sync,true} | Rest], FtOpt, EI) ->
    parse_ft_options(Rest, FtOpt#filetab_options{sync = true}, EI);
parse_ft_options([{sync,false} | Rest], FtOpt, EI) ->
    parse_ft_options(Rest, FtOpt, EI);
parse_ft_options([{extended_info,L} | Rest], FtOpt0, false) ->
    FtOpt1 = parse_ft_info_options(FtOpt0, L),
    parse_ft_options(Rest, FtOpt1, true);
parse_ft_options([Other | _], _, _) ->
    throw({unknown_option, Other});
parse_ft_options(Malformed, _, _) ->
    throw({malformed_option, Malformed}).

parse_ft_info_options(FtOpt,[]) ->
    FtOpt;
parse_ft_info_options(FtOpt,[object_count | T]) ->
    parse_ft_info_options(FtOpt#filetab_options{object_count = true}, T);
parse_ft_info_options(FtOpt,[md5sum | T]) ->
    parse_ft_info_options(FtOpt#filetab_options{md5sum = true}, T);
parse_ft_info_options(_,[Unexpected | _]) ->
    throw({unknown_option,[{extended_info,[Unexpected]}]});
parse_ft_info_options(_,Malformed) ->
    throw({malformed_option,Malformed}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Read a dumped file from disk and create a corresponding table
%% Opts := [Opt]
%% Opt := {verify,boolean()}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec file2tab(Filename) -> {'ok', Tab} | {'error', Reason} when
      Filename :: file:name(),
      Tab :: ets:tab(),
      Reason :: term().

file2tab(File) ->
    file2tab(File, []).

-spec file2tab(Filename, Options) -> {'ok', Tab} | {'error', Reason} when
      Filename :: file:name(),
      Tab :: ets:tab(),
      Options :: [Option],
      Option :: {'verify', boolean()},
      Reason :: term().

file2tab(File, Opts) ->
    try
        {ok,Verify,TabArg} = parse_f2t_opts(Opts,false,[]),
        Name = make_ref(),
        {ok, Name} =
        case disk_log:open([{name, Name},
                            {file, File},
                            {mode, read_only}]) of
            {ok, Name} ->
                {ok, Name};
            {repaired, Name, _,_} -> %Uh? cannot happen?
                case Verify of
                    true ->
                        _ = disk_log:close(Name),
                        throw(badfile);
                    false ->
                        {ok, Name}
                end;
            {error, Other1} ->
                throw({read_error, Other1});
            Other2 ->
                throw(Other2)
        end,
        {ok, Major, Minor, FtOptions, MD5State, FullHeader, DLContext} =
        try get_header_data(Name, Verify)
        catch
            badfile ->
                _ = disk_log:close(Name),
                throw(badfile)
        end,
        try
            if
                Major > ?MAJOR_F2T_VERSION ->
                    throw({unsupported_file_version,{Major,Minor}});
                true ->
                    ok
            end,
            {ok, Tab, HeadCount} = create_tab(FullHeader, TabArg),
            StrippedOptions =
            case Verify of
                true ->
                    FtOptions;
                false ->
                    #filetab_options{}
            end,
            {ReadFun,InitState} =
            case StrippedOptions#filetab_options.md5sum of
                true ->
                    {fun({OldMD5State,OldCount,_OL,ODLContext} = OS) ->
                             case wrap_bchunk(Name,ODLContext,100,Verify) of
                                 eof ->
                                     {OS,[]};
                                 {NDLContext,Blist} ->
                                     {Termlist, NewMD5State,
                                      NewCount,NewLast} =
                                     md5_and_convert(Blist,
                                                     OldMD5State,
                                                     OldCount),
                                     {{NewMD5State, NewCount,
                                       NewLast,NDLContext},
                                      Termlist}
                             end
                     end,
                     {MD5State,0,[],DLContext}};
                false ->
                    {fun({_,OldCount,_OL,ODLContext} = OS) ->
                             case wrap_chunk(Name,ODLContext,100,Verify) of
                                 eof ->
                                     {OS,[]};
                                 {NDLContext,List} ->
                                     {NewLast,NewCount,NewList} =
                                     scan_for_endinfo(List, OldCount),
                                     {{false,NewCount,NewLast,NDLContext},
                                      NewList}
                             end
                     end,
                     {false,0,[],DLContext}}
            end,
            try
                do_read_and_verify(ReadFun,InitState,Tab,
                                   StrippedOptions,HeadCount,Verify)
            catch
                throw:TReason ->
                    ets:delete(Tab),
                    throw(TReason);
                exit:ExReason ->
                    ets:delete(Tab),
                    exit(ExReason);
                error:ErReason ->
                    ets:delete(Tab),
                    erlang:raise(error,ErReason,erlang:get_stacktrace())
            end
        after
            _ = disk_log:close(Name)
        end
    catch
        throw:TReason2 ->
            {error,TReason2};
        exit:ExReason2 ->
            {error,ExReason2}
    end.

do_read_and_verify(ReadFun,InitState,Tab,FtOptions,HeadCount,Verify) ->
    case load_table(ReadFun,InitState,Tab) of
        {ok,{_,FinalCount,[],_}} ->
            case {FtOptions#filetab_options.md5sum,
                  FtOptions#filetab_options.object_count} of
                {false,false} ->
                    case Verify of
                        false ->
                            ok;
                        true ->
                            case FinalCount of
                                HeadCount ->
                                    ok;
                                _ ->
                                    throw(invalid_object_count)
                            end
                    end;
                _ ->
                    throw(badfile)
            end,
            {ok,Tab};
        {ok,{FinalMD5State,FinalCount,['$end_of_table',LastInfo],_}} ->
            ECount = case lists:keyfind(count,1,LastInfo) of
                         {count,N} ->
                             N;
                         _ ->
                             false
                     end,
            EMD5 = case lists:keyfind(md5,1,LastInfo) of
                       {md5,M} ->
                           M;
                       _ ->
                           false
                   end,
            case FtOptions#filetab_options.md5sum of
                true ->
                    case erlang:md5_final(FinalMD5State) of
                        EMD5 ->
                            ok;
                        _MD5MisM ->
                            throw(checksum_error)
                    end;
                false ->
                    ok
            end,
            case FtOptions#filetab_options.object_count of
                true ->
                    case FinalCount of
                        ECount ->
                            ok;
                        _Other ->
                            throw(invalid_object_count)
                    end;
                false ->
                    %% Only use header count if no extended info
                    %% at all is present and verification is requested.
                    case {Verify,FtOptions#filetab_options.md5sum} of
                        {true,false} ->
                            case FinalCount of
                                HeadCount ->
                                    ok;
                                _Other2 ->
                                    throw(invalid_object_count)
                            end;
                        _ ->
                            ok
                    end
            end,
            {ok,Tab}
    end.

parse_f2t_opts([],Verify,Tab) ->
    {ok,Verify,Tab};
parse_f2t_opts([{verify, true}|T],_OV,Tab) ->
    parse_f2t_opts(T,true,Tab);
parse_f2t_opts([{verify,false}|T],OV,Tab) ->
    parse_f2t_opts(T,OV,Tab);
parse_f2t_opts([{table,Tab}|T],OV,[]) ->
    parse_f2t_opts(T,OV,Tab);
parse_f2t_opts([Unexpected|_],_,_) ->
    throw({unknown_option,Unexpected});
parse_f2t_opts(Malformed,_,_) ->
    throw({malformed_option,Malformed}).

count_mandatory([]) ->
    0;
count_mandatory([{Tag,_}|T]) when Tag =:= name;
                                  Tag =:= type;
                                  Tag =:= protection;
                                  Tag =:= named_table;
                                  Tag =:= keypos;
                                  Tag =:= size ->
    1+count_mandatory(T);
count_mandatory([_|T]) ->
    count_mandatory(T).

verify_header_mandatory(L) ->
    count_mandatory(L) =:= 6.

wrap_bchunk(Name,C,N,true) ->
    case disk_log:bchunk(Name,C,N) of
        {_,_,X} when X > 0 ->
            throw(badfile);
        {NC,Bin,_} ->
            {NC,Bin};
        Y ->
            Y
    end;
wrap_bchunk(Name,C,N,false) ->
    case disk_log:bchunk(Name,C,N) of
        {NC,Bin,_} ->
            {NC,Bin};
        Y ->
            Y
    end.

wrap_chunk(Name,C,N,true) ->
    case disk_log:chunk(Name,C,N) of
        {_,_,X} when X > 0 ->
            throw(badfile);
        {NC,TL,_} ->
            {NC,TL};
        Y ->
            Y
    end;
wrap_chunk(Name,C,N,false) ->
    case disk_log:chunk(Name,C,N) of
        {NC,TL,_} ->
            {NC,TL};
        Y ->
            Y
    end.

get_header_data(Name,true) ->
    case wrap_bchunk(Name,start,1,true) of
        {C,[Bin]} when is_binary(Bin) ->
            T = binary_to_term(Bin),
            case T of
                Tup when is_tuple(Tup) ->
                    L = tuple_to_list(Tup),
                    case verify_header_mandatory(L) of
                        false ->
                            throw(badfile);
                        true ->
                            Major = case lists:keyfind(major,1,L) of
                                        {major,Maj} ->
                                            Maj;
                                        _ ->
                                            0
                                    end,
                            Minor = case lists:keyfind(minor,1,L) of
                                        {minor,Min} ->
                                            Min;
                                        _ ->
                                            0
                                    end,
                            FtOptions =
                            case lists:keyfind(extended_info,1,L) of
                                {extended_info,I} when is_list(I) ->
                                    #filetab_options
                                    {
                                     object_count =
                                     lists:member(object_count,I),
                                     md5sum =
                                     lists:member(md5sum,I)
                                    };
                                _ ->
                                    #filetab_options{}
                            end,
                            MD5Initial =
                            case FtOptions#filetab_options.md5sum of
                                true ->
                                    X = erlang:md5_init(),
                                    erlang:md5_update(X,Bin);
                                false ->
                                    false
                            end,
                            {ok, Major, Minor, FtOptions, MD5Initial, L, C}
                    end;
                _X ->
                    throw(badfile)
            end;
        _Y ->
            throw(badfile)
    end;

get_header_data(Name, false) ->
    case wrap_chunk(Name, start, 1, false) of
        {C,[Tup]} when is_tuple(Tup) ->
            L = tuple_to_list(Tup),
            case verify_header_mandatory(L) of
                false ->
                    throw(badfile);
                true ->
                    Major = case lists:keyfind(major_version, 1, L) of
                                {major_version, Maj} ->
                                    Maj;
                                _ ->
                                    0
                            end,
                    Minor = case lists:keyfind(minor_version, 1, L) of
                                {minor_version, Min} ->
                                    Min;
                                _ ->
                                    0
                            end,
                    FtOptions =
                    case lists:keyfind(extended_info, 1, L) of
                        {extended_info, I} when is_list(I) ->
                            #filetab_options
                            {
                             object_count =
                             lists:member(object_count,I),
                             md5sum =
                             lists:member(md5sum,I)
                            };
                        _ ->
                            #filetab_options{}
                    end,
                    {ok, Major, Minor, FtOptions, false, L, C}
            end;
        _ ->
            throw(badfile)
    end.

md5_and_convert([], MD5State, Count) ->
    {[],MD5State,Count,[]};
md5_and_convert([H|T], MD5State, Count) when is_binary(H) ->
    case (catch binary_to_term(H)) of
        {'EXIT', _} ->
            md5_and_convert(T,MD5State,Count);
        ['$end_of_table',_Dat] = L ->
            {[],MD5State,Count,L};
        Term ->
            X = erlang:md5_update(MD5State, H),
            {Rest,NewMD5,NewCount,NewLast} = md5_and_convert(T, X, Count+1),
            {[Term | Rest],NewMD5,NewCount,NewLast}
    end.

scan_for_endinfo([], Count) ->
    {[],Count,[]};
scan_for_endinfo([['$end_of_table',Dat]], Count) ->
    {['$end_of_table',Dat],Count,[]};
scan_for_endinfo([Term|T], Count) ->
    {NewLast,NCount,Rest} = scan_for_endinfo(T, Count+1),
    {NewLast,NCount,[Term | Rest]}.

load_table(ReadFun, State, Tab) ->
    {NewState,NewData} = ReadFun(State),
    case NewData of
        [] ->
            {ok,NewState};
        List ->
            ets:insert(Tab, List),
            load_table(ReadFun, NewState, Tab)
    end.

create_tab(I, TabArg) ->
    {name, Name} = lists:keyfind(name, 1, I),
    {type, Type} = lists:keyfind(type, 1, I),
    {protection, P} = lists:keyfind(protection, 1, I),
    {keypos, _Kp} = Keypos = lists:keyfind(keypos, 1, I),
    {size, Sz} = lists:keyfind(size, 1, I),
    L1 = [Type, P, Keypos],
    L2 = case lists:keyfind(named_table, 1, I) of
             {named_table, true} -> [named_table | L1];
             {named_table, false} -> L1
         end,
    L3 = case lists:keyfind(compressed, 1, I) of
             {compressed, true} -> [compressed | L2];
             {compressed, false} -> L2;
             false -> L2
         end,
    L4 = case lists:keyfind(write_concurrency, 1, I) of
             {write_concurrency, _}=Wcc -> [Wcc | L3];
             _ -> L3
         end,
    L5 = case lists:keyfind(read_concurrency, 1, I) of
             {read_concurrency, _}=Rcc -> [Rcc | L4];
             false -> L4
         end,
    case TabArg of
        [] ->
            try
                Tab = ets:new(Name, L5),
                {ok, Tab, Sz}
            catch _:_ ->
                      throw(cannot_create_table)
            end;
        _ ->
            {ok, TabArg, Sz}
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% tabfile_info/1 reads the head information in an ets table dumped to
%% disk by means of file2tab and returns a list of the relevant table
%% information
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec tabfile_info(Filename) -> {'ok', TableInfo} | {'error', Reason} when
      Filename :: file:name(),
      TableInfo :: [InfoItem],
      InfoItem :: {'name', atom()}
      | {'type', Type}
      | {'protection', Protection}
      | {'named_table', boolean()}
      | {'keypos', non_neg_integer()}
      | {'size', non_neg_integer()}
      | {'extended_info', [ExtInfo]}
      | {'version', {Major :: non_neg_integer(),
                     Minor :: non_neg_integer()}},
      ExtInfo :: 'md5sum' | 'object_count',
      Type :: 'bag' | 'duplicate_bag' | 'ordered_set' | 'set',
      Protection :: 'private' | 'protected' | 'public',
      Reason :: term().

tabfile_info(File) when is_list(File) ; is_atom(File) ->
    try
        Name = make_ref(),
        {ok, Name} =
        case disk_log:open([{name, Name},
                            {file, File},
                            {mode, read_only}]) of
            {ok, Name} ->
                {ok, Name};
            {repaired, Name, _,_} -> %Uh? cannot happen?
                {ok, Name};
            {error, Other1} ->
                throw({read_error, Other1});
            Other2 ->
                throw(Other2)
        end,
        {ok, Major, Minor, _FtOptions, _MD5State, FullHeader, _DLContext} =
        try get_header_data(Name, false)
        catch
            badfile ->
                _ = disk_log:close(Name),
                throw(badfile)
        end,
        case disk_log:close(Name) of
            ok -> ok;
            {error, Reason} -> throw(Reason)
        end,
        {value, N} = lists:keysearch(name, 1, FullHeader),
        {value, Type} = lists:keysearch(type, 1, FullHeader),
        {value, P} = lists:keysearch(protection, 1, FullHeader),
        {value, Val} = lists:keysearch(named_table, 1, FullHeader),
        {value, Kp} = lists:keysearch(keypos, 1, FullHeader),
        {value, Sz} = lists:keysearch(size, 1, FullHeader),
        Ei = case lists:keyfind(extended_info, 1, FullHeader) of
                 false -> {extended_info, []};
                 Ei0 -> Ei0
             end,
        {ok, [N,Type,P,Val,Kp,Sz,Ei,{version,{Major,Minor}}]}
    catch
        throw:TReason ->
            {error,TReason};
        exit:ExReason ->
            {error,ExReason}
    end.
