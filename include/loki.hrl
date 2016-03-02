-record(store, {name,               % Name of the loki instance
                backend,            % Backend used
                lock :: boolean(),  % Boolean indicating whether locks are required
                config,             % Config specific to the backend
                options             % Additional options
               }).
