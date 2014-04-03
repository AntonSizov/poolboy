%% Poolboy - A hunky Erlang worker pool factory

-module(pb_poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, start/1, start/2,
         start_link/1, start_link/2, stop/1, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TIMEOUT, 5000).

-record(state, {
    supervisor :: pid(),
    workers :: queue(),
    waiting :: queue(),
    monitors :: ets:tid(),
    max_size = 5 :: non_neg_integer(), %% max static pool size
	size = 0 :: non_neg_integer(), %% current static pool size
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer()
}).

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
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{max_size = Size, supervisor = Sup} = State) ->
    Workers = prepopulate(Size, Sup),
    {ok, State#state{size = queue:len(Workers), workers = Workers}}.

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

handle_call({checkout, Block}, {FromPid, _} = From, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow,
		   size = Size,
		   max_size = MaxSize} = State,
    case queue:out(Workers) of
        {{value, Pid}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            {reply, Pid, State#state{workers = Left}};
		%% pool for some reason is not full
        {empty, Empty} when MaxSize > 0, Size < MaxSize ->
			case new_worker(Sup) of
				{ok, Pid} ->
				    Ref = erlang:monitor(process, FromPid),
		            true = ets:insert(Monitors, {Pid, Ref}),
		            {reply, Pid, State#state{workers = Empty, size = Size + 1}};
				{error, _Reason} when Block =:= false ->
		            {reply, full, State#state{workers = Empty}};
				{error, _Reason} ->
				    Ref = erlang:monitor(process, FromPid),
		            Waiting = queue:in({From, Ref}, State#state.waiting),
		            {noreply, State#state{workers = Empty, waiting = Waiting}}
			end;
        {empty, Empty} when MaxOverflow > 0, Overflow < MaxOverflow ->
			case new_worker(Sup) of
				{ok, Pid} ->
				    Ref = erlang:monitor(process, FromPid),
		            true = ets:insert(Monitors, {Pid, Ref}),
		            {reply, Pid, State#state{workers = Empty, overflow = Overflow + 1}};
				{error, _Reason} when Block =:= false ->
		            {reply, full, State#state{workers = Empty}};
				{error, _Reason} ->
				    Ref = erlang:monitor(process, FromPid),
		            Waiting = queue:in({From, Ref}, State#state.waiting),
		            {noreply, State#state{workers = Empty, waiting = Waiting}}
			end;
        {empty, Empty} when Block =:= false ->
            {reply, full, State#state{workers = Empty}};
        {empty, Empty} ->
            Ref = erlang:monitor(process, FromPid),
            Waiting = queue:in({From, Ref}, State#state.waiting),
            {noreply, State#state{workers = Empty, waiting = Waiting}}
    end;

handle_call(status, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
		   supervisor = Sup,
		   size = Size} = State,
    StateName = state_name(State),
	SupChilds = proplists:get_value(active, supervisor:count_children(Sup)),
	Status = [
		{state, StateName},
		{workers, queue:len(Workers)},
		{size, Size},
		{overflow, Overflow},
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
handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        [] ->
            case queue:member(Pid, State#state.workers) of
                true ->
                    W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
					case new_worker(Sup) of
						{ok, NewPid} ->
		                    {noreply, State#state{workers = queue:in(NewPid, W)}};
						{error, _Reason} ->
							Size = State#state.size,
							{noreply, State#state{workers = W, size = Size - 1}}
					end;
                false ->
					%% worker pid should be in monitors' list or in ready queue
					%% if not, it is abnormous
					error_logger:warning_msg("Got exit signal from worker not in"
						" monitors' list nor in ready queue either"),
                    {noreply, State}
            end
    end;

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

start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_server:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_server:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

new_worker(Sup) ->
    case supervisor:start_child(Sup, []) of
		{ok, Pid} ->
		    true = link(Pid),
			{ok, Pid};
		{error, Reason} ->
			error_logger:warning_msg("Can't start worker: ~p", [Reason]),
			{error, Reason}
	end.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

prepopulate(N, _Sup) when N < 1 ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

prepopulate(N, _Sup, Workers) when N < 1 ->
    Workers;
prepopulate(N, Sup, Workers) ->
	case new_worker(Sup) of
		{ok, NewWorker} ->
		    prepopulate(N-1, Sup, queue:in(NewWorker, Workers));
		{error, _Reason} ->
		    prepopulate(N-1, Sup, Workers)
	end.

handle_checkin(Pid, State) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           overflow = Overflow} = State,
    case queue:out(Waiting) of
        {{value, {{FromPid, _} = From, _}}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            gen_server:reply(From, Pid),
            State#state{waiting = Left};
        {empty, Empty} when Overflow > 0 ->
            ok = dismiss_worker(Sup, Pid),
            State#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            Workers = queue:in(Pid, State#state.workers),
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end.

handle_worker_exit(Pid, State) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           overflow = Overflow,
		   size = Size} = State,
    case queue:out(State#state.waiting) of
        {{value, {{FromPid, _} = From, _}}, LeftWaiting} ->
            case new_worker(State#state.supervisor) of
				{ok, NewWorker} ->
		            MonitorRef = erlang:monitor(process, FromPid),
		            true = ets:insert(Monitors, {NewWorker, MonitorRef}),
		            gen_server:reply(From, NewWorker),
		            State#state{waiting = LeftWaiting};
				{error, _Reason} when Overflow > 0 ->
					State#state{overflow = Overflow - 1};
				{error, _Reason} ->
					State#state{size = Size - 1}
			end;
        {empty, Empty} when Overflow > 0 ->
            State#state{overflow = Overflow - 1, waiting = Empty};
        {empty, Empty} ->
            Workers = queue:in(
                new_worker(Sup),
                queue:filter(fun (P) -> P =/= Pid end, State#state.workers)
            ),
            State#state{workers = Workers, waiting = Empty}
    end.

state_name(State = #state{overflow = Overflow}) when Overflow < 1 ->
    #state{max_overflow = MaxOverflow, workers = Workers} = State,
    case queue:len(Workers) == 0 of
        true when MaxOverflow < 1 -> full;
        true -> overflow;
        false -> ready
    end;
state_name(#state{overflow = MaxOverflow, max_overflow = MaxOverflow}) ->
    full;
state_name(_State) ->
    overflow.
