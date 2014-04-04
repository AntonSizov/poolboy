%% Poolboy - A hunky Erlang worker pool factory

-module(pb_poolboy).

-behaviour(gen_server).

% API
-export([
	checkout/1, checkout/2, checkout/3,
	checkin/2,
	transaction/2, transaction/3,
	child_spec/2, child_spec/3,
	start/1, start/2,
	start_link/1, start_link/2,
	stop/1,
	status/1
]).

% gen_server callbacks
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-define(TIMEOUT, 5000).
-record(create_worker, {}).

-record(state, {
    supervisor :: pid(),
    workers :: queue(),
    waiting :: queue(),
    monitors :: ets:tid(),
    max_size = 5 :: non_neg_integer(), %% max static pool size
	size = 0 :: non_neg_integer(), %% current static pool size
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
	scheduled = false :: boolean(), %% is new worker create scheduled or not
	dismiss = true :: boolean()
}).

%% ===================================================================
%% API
%% ===================================================================

-spec checkout(Pool :: node()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: node(), Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: node(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, Timeout) ->
    try
        gen_server:call(Pool, {checkout, Block}, Timeout)
    catch
        Class:Reason ->
            gen_server:cast(Pool, {cancel_waiting, self()}),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.

-spec checkin(Pool :: node(), Worker :: pid()) -> ok.
checkin(Pool, Worker) when is_pid(Worker) ->
    gen_server:cast(Pool, {checkin, Worker}).

-spec transaction(Pool :: node(), Fun :: fun((Worker :: pid()) -> any()))
    -> any().
transaction(Pool, Fun) ->
    transaction(Pool, Fun, ?TIMEOUT).

-spec transaction(Pool :: node(), Fun :: fun((Worker :: pid()) -> any()),
    Timeout :: timeout()) -> any().
transaction(Pool, Fun, Timeout) ->
    Worker = pb_poolboy:checkout(Pool, true, Timeout),
    try
        Fun(Worker)
    after
        ok = pb_poolboy:checkin(Pool, Worker)
    end.

-spec child_spec(Pool :: node(), PoolArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs) ->
    child_spec(Pool, PoolArgs, []).

-spec child_spec(Pool :: node(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs, WorkerArgs) ->
    {Pool, {pb_poolboy, start_link, [PoolArgs, WorkerArgs]},
     permanent, 5000, worker, [pb_poolboy]}.

-spec start(PoolArgs :: proplists:proplist())
    -> gen:start_ret().
start(PoolArgs) ->
    start(PoolArgs, PoolArgs).

-spec start(PoolArgs :: proplists:proplist(),
            WorkerArgs:: proplists:proplist())
    -> gen:start_ret().
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).

-spec start_link(PoolArgs :: proplists:proplist())
    -> gen:start_ret().
start_link(PoolArgs)  ->
    %% for backwards compatability, pass the pool args as the worker args as well
    start_link(PoolArgs, PoolArgs).

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs:: proplists:proplist())
    -> gen:start_ret().
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

-spec stop(Pool :: node()) -> ok.
stop(Pool) ->
    gen_server:call(Pool, stop).

-spec status(Pool :: node()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
    gen_server:call(Pool, status).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting = Waiting, monitors = Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
    {ok, Sup} = pb_poolboy_sup:start_link(Mod, WorkerArgs),
    init(Rest, WorkerArgs, State#state{supervisor = Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{max_size = Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State) when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow});
init([{dismiss_overflow, Dismiss} | Rest], WorkerArgs, State) when is_boolean(Dismiss) ->
	init(Rest, WorkerArgs, State#state{dismiss = Dismiss});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{max_size = MaxSize} = State) ->
    Workers = prepopulate(MaxSize, State),
	Size = queue:len(Workers),
	Scheduled =
	case Size < MaxSize of
		true -> schedule_worker_create(State), true;
		false -> false
	end,
    {ok, State#state{
			size = Size,
			workers = Workers,
			scheduled = Scheduled
	}}.

handle_cast({checkin, Pid}, State = #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast({cancel_waiting, Pid}, State) ->
    Waiting = queue:filter(fun ({{P, _}, _}) -> P =/= Pid end, State#state.waiting),
    {noreply, State#state{waiting = Waiting}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call({checkout, Block}, From, State) ->
	NewState = handle_checkout(Block, From, State),
	{noreply, NewState};

handle_call(status, _From, State) ->
    #state{workers = Workers,
		   dismiss = Dismiss,
		   scheduled = Scheduled,
           monitors = Monitors,
           overflow = Overflow,
		   max_overflow = MaxOverflow,
		   supervisor = Sup,
		   size = Size,
		   max_size = MaxSize} = State,
	SupChilds = proplists:get_value(active, supervisor:count_children(Sup)),
	Status = [
		{dismiss, Dismiss},
		{scheduled, Scheduled},
		{size, Size},
		{max_size, MaxSize},
		{overflow, Overflow},
		{max_overflow, MaxOverflow},
		{workers, queue:len(Workers)},
		{monitors, ets:info(Monitors, size)},
		{sup_childs, SupChilds}
	],
    {reply, {ok, Status}, State};
handle_call(get_avail_workers, _From, State) ->
    Workers = State#state.workers,
    WorkerList = queue:to_list(Workers),
    {reply, WorkerList, State};
handle_call(get_all_workers, _From, State) ->
    Sup = State#state.supervisor,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, State};
handle_call(get_all_monitors, _From, State) ->
    Monitors = ets:tab2list(State#state.monitors),
    {reply, Monitors, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, State}.

handle_info(#create_worker{}, State0 = #state{}) ->
	State1 = handle_unlock_create_workers(State0),
	{noreply, State1};

handle_info({'DOWN', Ref, _, _, _}, State) ->
    case ets:match(State#state.monitors, {'$1', Ref}) of
        [[Pid]] ->
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            Waiting = queue:filter(fun ({_, R}) -> R =/= Ref end, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;
handle_info({'EXIT', Pid, Reason}, State = #state{supervisor = Pid}) ->
	{stop, Reason, State};
handle_info({'EXIT', Pid, _Reason}, State0) ->
	State1 = handle_worker_exit(cleanup_monitors, Pid, State0),
	{noreply, State1};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State = #state{workers = WorkersQ, monitors = Monitors}) ->
	%% unlink workers in queue
	[unlink(Pid) || Pid <- queue:to_list(WorkersQ)],
	%% unlink workers in monitors
	[unlink(Pid) || {Pid, _Ref} <- ets:tab2list(Monitors)],
    true = exit(State#state.supervisor, shutdown).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internals
%% ===================================================================

start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_server:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_server:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

prepopulate(N, _State) when N < 1 ->
    queue:new();
prepopulate(N, State) ->
    prepopulate(N, State, queue:new()).

prepopulate(N, _State, Workers) when N < 1 ->
    Workers;
prepopulate(N, State, Workers) ->
	case new_worker(static, State) of
		{ok, NewWorker} ->
		    prepopulate(N-1, State, queue:in(NewWorker, Workers));
		{error, _Reason} ->
		    prepopulate(N-1, State, Workers)
	end.

handle_checkin(Pid, State) ->
    #state{supervisor = Sup,
		   waiting = Waiting,
           overflow = Overflow,
		   dismiss = Dismiss} = State,
    case queue:out(Waiting) of
        {{value, {From, _}}, Left} ->
			delegate_worker(Pid, From, State),
            State#state{waiting = Left};
        {empty, Empty} when Overflow > 0, Dismiss == true ->
            ok = dismiss_worker(Sup, Pid),
            State#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            Workers = queue:in(Pid, State#state.workers),
            State#state{workers = Workers, waiting = Empty}
    end.

%% ===================================================================
%% Handle checkout
%% ===================================================================

handle_checkout(Block, From, State0) ->
	case get_worker(State0) of
		{ok, Pid, State1} ->
			delegate_worker(Pid, From, State1),
			State1;
		empty ->
			handle_checkout(can_create_worker, Block, From, State0)
	end.
handle_checkout(can_create_worker, Block, From, State0) ->
	case can_create_worker(State0) of
		true -> handle_checkout(try_create_worker, Block, From, State0);
		false when Block =:= false ->
			gen_server:reply(From, full),
			State0;
		false when Block =:= true ->
			request_to_waitings(From, State0)
	end;
handle_checkout(try_create_worker, Block, From, State0) ->
	Type = new_worker_type(State0),
	case new_worker(Type, State0) of
		{ok, Pid} ->
			State1 = increment_worker_counter(State0),
			delegate_worker(Pid, From, State1),
			State1;
		{error, _Reason} when Block =:= false ->
			schedule_worker_create(State0),
			gen_server:reply(From, full),
			State0#state{scheduled = true};
		{error, _Reason} when Block =:= true ->
			schedule_worker_create(State0),
			State1 = request_to_waitings(From, State0),
			State1#state{scheduled = true}
	end.

%% Handle checkout helpers

request_to_waitings({FromPid, _} = From, State) ->
    Ref = erlang:monitor(process, FromPid),
	Waiting = queue:in({From, Ref}, State#state.waiting),
	State#state{waiting = Waiting}.

get_worker(State = #state{workers = Workers}) ->
    case queue:out(Workers) of
        {{value, Pid}, Left} ->
			{ok, Pid, State#state{workers = Left}};
        {empty, _Empty} ->
			empty
	end.

%% ===================================================================
%% Handle create worker
%% ===================================================================

handle_unlock_create_workers(State0) ->
	handle_unlock_create_workers(is_there_waiting, State0#state{scheduled = false}).

handle_unlock_create_workers(is_there_waiting, State0) ->
	case queue:is_empty(State0#state.waiting) of
		false -> handle_unlock_create_workers(can_create_worker, State0);
		true -> State0
	end;
handle_unlock_create_workers(can_create_worker, State0) ->
	case can_create_worker(State0) of
		true -> handle_unlock_create_workers(try_create_worker, State0);
		false -> State0
	end;
handle_unlock_create_workers(try_create_worker, State0) ->
	Type = new_worker_type(State0),
	case new_worker(Type, State0) of
		{ok, Pid} ->
			State1 = increment_worker_counter(State0),
			{ok, From, State2} = get_waiting(State1),
			delegate_worker(Pid, From, State2),
			State2;
		{error, _Reason} ->
			schedule_worker_create(State0),
			State0#state{scheduled = true}
	end.

get_waiting(State = #state{waiting = Waiting}) ->
	case queue:out(Waiting) of
		{{value, {From, _}}, Left} ->
			{ok, From, State#state{waiting = Left}};
		{empty, _Empty} -> empty
	end.

%% ===================================================================
%% Handle worker exit
%% ===================================================================

handle_worker_exit(cleanup_monitors, Pid, State0) ->
	Monitors = State0#state.monitors,
	case ets:lookup(Monitors, Pid) of
		[{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
			State1 = decrement_worker_counter(State0),
			handle_worker_exit(is_there_waiting, State1);
		[] ->
			handle_worker_exit(cleanup_ready_queue, State0)
	end;
handle_worker_exit(cleanup_ready_queue, Pid, State0) ->
	case queue:member(Pid, State0#state.workers) of
		true ->
			State1 = decrement_worker_counter(State0),
			FilteredWorkers =
				queue:filter(fun (P) -> P =/= Pid end, State1#state.workers),
			State2 = State1#state{workers = FilteredWorkers},
			handle_worker_exit(is_there_waiting, State2);
		false ->
			%% skip counter decrement as this case can be
			%% result of worker dismiss
			handle_worker_exit(is_there_waiting, State0)
	end.

handle_worker_exit(is_there_waiting, State0) ->
	case queue:is_empty(State0#state.waiting) of
		false -> handle_worker_exit(can_create_worker, State0);
		true -> State0
	end;
handle_worker_exit(can_create_worker, State0) ->
	case can_create_worker(State0) of
		true -> handle_worker_exit(try_create_worker, State0);
		false -> State0
	end;
handle_worker_exit(try_create_worker, State0) ->
	Type = new_worker_type(State0),
	case new_worker(Type, State0) of
		{ok, Pid} ->
			State1 = increment_worker_counter(State0),
			{ok, From, State2} = get_waiting(State1),
			delegate_worker(Pid, From, State2),
			State2;
		{error, _Reason} ->
			schedule_worker_create(State0),
			State0#state{scheduled = true}
	end.

%% ===================================================================
%% Common helpers
%% ===================================================================

can_create_worker(#state{scheduled = false, size = S, max_size = MS}) when
		S < MS -> true;
can_create_worker(#state{scheduled = false, overflow = O, max_overflow = MO}) when
		O < MO -> true;
can_create_worker(#state{}) -> false.

schedule_worker_create(#state{scheduled = true}) -> ok;
schedule_worker_create(#state{scheduled = false}) ->
	erlang:send_after(1000, self(), #create_worker{}).

new_worker_type(#state{size = Size, max_size = MaxSize})
		when Size < MaxSize -> static;
new_worker_type(#state{overflow = Overflow, max_overflow = MaxOverflow})
		when Overflow < MaxOverflow -> overflow.

%% if auto dismiss overflow workers is disabled, notify workers
%% that it started as overflow
new_worker(overflow, #state{supervisor = Sup, dismiss = false}) ->
    case supervisor:start_child(Sup, [overflow]) of
		{ok, Pid} ->
		    true = link(Pid),
			{ok, Pid};
		{error, Reason} ->
			{error, Reason}
	end;
new_worker(_, #state{supervisor = Sup}) ->
    case supervisor:start_child(Sup, []) of
		{ok, Pid} ->
		    true = link(Pid),
			{ok, Pid};
		{error, Reason} ->
			{error, Reason}
	end.

increment_worker_counter(State = #state{size = S, max_size = MS}) when
		S < MS ->
	State#state{size = S + 1};
increment_worker_counter(State = #state{overflow = O, max_overflow = MO}) when
		O < MO ->
	State#state{overflow = O + 1}.

decrement_worker_counter(State = #state{overflow = Overflow}) when
		Overflow > 0 ->
	State#state{overflow = Overflow - 1};
decrement_worker_counter(State = #state{size = Size}) ->
	State#state{size = Size - 1}.

delegate_worker(Pid, {FromPid, _} = From, State) ->
	Ref = erlang:monitor(process, FromPid),
	true = ets:insert(State#state.monitors, {Pid, Ref}),
	gen_server:reply(From, Pid).
